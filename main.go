// Reproduction of https://github.com/fluxcd/helm-controller/issues/1409
//
// Each attempt:
//  1. Creates a number of HelmReleases
//  2. Waits until `Ready` status condition message contains "Running 'install' action"
//  3. Storms annotation patches so resourceVersion outruns the informer cache
//     while helm-controller tries to patch `Released=True` and `Ready=True`
//  4. Asks helm-controller to reconcile. If `Ready` is still Unknown, `Released` is missing,
//     but release history shows `deployed`, that is a hit.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	namespace          = "reproduce-issue-1409"
	namePrefix         = "repro-hr"
	verificationProbes = 5
	installMarker      = "Running 'install' action"
	podinfoRepo        = "podinfo"
)

var (
	helmReleaseCount  = flag.Int("helm-release-count", 1, "HelmReleases installed concurrently per attempt")
	patchStormWorkers = flag.Int("patch-storm-workers", 8, "concurrent patchers per HelmRelease during the burst")
	maxAttempts       = flag.Int("max-attempts", 10, "maximum attempts")
	caBundlePath      = flag.String("ca-bundle-path", os.Getenv("CA_BUNDLE_PATH"), "CA bundle to use (in case of TLS-intercepting proxies)")
	stormDuration     = 8 * time.Second
	installWait       = time.Minute
	probeTimeout      = 2 * time.Minute
)

type repro struct {
	client client.Client
	log    *slog.Logger
	name   string

	stuck  bool
	probes int
	obj    *helmv2.HelmRelease
	err    error
}

type workerStat struct {
	success, error atomic.Int64
}

func main() {
	flag.Parse()

	c := mustClient()
	ctx := context.Background()

	if err := ensureNamespace(ctx, c); err != nil {
		fatal("ensure namespace", err)
	}
	if err := ensureCABundle(ctx, c); err != nil {
		fatal("apply ca bundle", err)
	}
	if err := ensureHelmRepository(ctx, c); err != nil {
		fatal("apply helmrepository", err)
	}

	hrs := make([]repro, *helmReleaseCount)
	for i := range hrs {
		hrs[i] = repro{
			client: c,
			name:   fmt.Sprintf("%s-%d", namePrefix, i),
		}
	}
	for attempt := 1; attempt <= *maxAttempts; attempt++ {
		log := slog.With("iteration", attempt)
		log.Info("starting attempt", "helmreleases", *helmReleaseCount, "workers", *patchStormWorkers, "burst", stormDuration)

		var wg sync.WaitGroup
		for i := range hrs {
			wg.Add(1)
			go func(r *repro) {
				defer wg.Done()
				r.log = log
				r.delete(ctx)
				r.run(ctx)
			}(&hrs[i])
		}
		wg.Wait()

		for i := range hrs {
			hrs[i].report()
			if hrs[i].isStuck() {
				os.Exit(0)
			}
		}
	}

	for i := range hrs {
		hrs[i].log = slog.Default()
		hrs[i].delete(ctx)
	}
	slog.Info("issue was not reproduced")
	os.Exit(1)
}

func (r *repro) run(ctx context.Context) {
	r.stuck = false
	r.probes = 0
	r.obj = nil
	r.err = nil

	if err := r.install(ctx); err != nil {
		r.err = err
		return
	}
	if err := r.awaitInstallMarker(ctx); err != nil {
		r.err = err
		return
	}
	r.patchStorm(ctx)
	r.verifyIssueReproduced(ctx)
}

func (r *repro) install(ctx context.Context) error {
	helmrelease := &helmv2.HelmRelease{
		ObjectMeta: metav1.ObjectMeta{Name: r.name, Namespace: namespace},
		Spec: helmv2.HelmReleaseSpec{
			Interval: metav1.Duration{Duration: 10 * time.Minute},
			Install: &helmv2.Install{
				DisableWait: true,
			},
			Chart: &helmv2.HelmChartTemplate{
				Spec: helmv2.HelmChartTemplateSpec{
					Chart:   "podinfo",
					Version: "6.5.0",
					SourceRef: helmv2.CrossNamespaceObjectReference{
						Kind: sourcev1.HelmRepositoryKind,
						Name: podinfoRepo,
					},
				},
			},
		},
	}

	r.log.Info("installing helmrelease", "helmrelease", r.name)
	err := r.client.Create(ctx, helmrelease)
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	return nil
}

// awaitInstallMarker waits for patch #1 (Ready=Unknown "Running 'install' action").
// The storm must not start earlier: that would also fail patch #1, and then
// there is no stale Ready=Unknown to strand.
func (r *repro) awaitInstallMarker(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, installWait)
	defer cancel()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("install marker not seen within %s", installWait)
		case <-tick.C:
			obj, err := r.get(ctx)
			if err != nil {
				continue
			}
			if _, msg := condition(obj.Status.Conditions, meta.ReadyCondition); strings.Contains(msg, installMarker) {
				return nil
			}
		}
	}
}

// patchStorm runs annotation merge patches until stormDuration elapses. Merge
// patches always bump resourceVersion; a no-op server-side apply does not.
func (r *repro) patchStorm(ctx context.Context) {
	r.log.Info("patch storm starting", "helmrelease", r.name)
	stormCtx, stop := context.WithTimeout(ctx, stormDuration)
	stats := make([]workerStat, *patchStormWorkers)
	logged := r.logStormProgress(stormCtx, stats)

	var wg sync.WaitGroup
	for w := 0; w < *patchStormWorkers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r.patchStormWorker(stormCtx, w, &stats[w])
		}(w)
	}
	wg.Wait()
	stop()
	<-logged
	var success, errors int64
	for i := range stats {
		success += stats[i].success.Load()
		errors += stats[i].error.Load()
	}
	r.log.Info("patch storm finished", "helmrelease", r.name, "success", success, "error", errors)
}

func (r *repro) patchStormWorker(ctx context.Context, worker int, st *workerStat) {
	key := fmt.Sprintf("%s/w%d", namePrefix, worker)
	for n := int64(1); ctx.Err() == nil; n++ {
		err := r.patchAnnotations(ctx, map[string]string{key: fmt.Sprint(n)})
		if err == nil {
			st.success.Add(1)
			continue
		}
		if apierrors.IsNotFound(err) {
			return
		}
		if apierrors.IsConflict(err) {
			st.error.Add(1)
		}
	}
}

// verifyIssueReproduced requests full reconciliation via HelmRelease annotations, and records
// whether the issue is still visible after each ack.
func (r *repro) verifyIssueReproduced(ctx context.Context) {
	for i := 0; i < verificationProbes; i++ {
		token := fmt.Sprintf("repro-%d-%d", i, time.Now().UnixNano())
		if err := r.patchAnnotations(ctx, map[string]string{meta.ReconcileRequestAnnotation: token}); err != nil {
			r.err = fmt.Errorf("request reconcile: %w", err)
			return
		}
		obj, err := r.awaitReconcileAck(ctx, token)
		r.obj = obj
		if err != nil {
			r.err = err
			return
		}
		r.probes = i + 1
		if !isStuck(obj) {
			return
		}
	}
	r.stuck = true
}

func (r *repro) delete(ctx context.Context) {
	r.log.Info("deleting helmrelease", "helmrelease", r.name)
	err := r.client.Delete(ctx, &helmv2.HelmRelease{
		ObjectMeta: metav1.ObjectMeta{Name: r.name, Namespace: namespace},
	})
	if err != nil && !apierrors.IsNotFound(err) {
		r.log.Error("delete helmrelease", "helmrelease", r.name, "error", err)
	}

	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		_, err = r.get(ctx)
		if apierrors.IsNotFound(err) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (r *repro) report() {
	if r.isStuck() {
		r.log.Info("issue was reproduced")
	} else {
		r.log.Info("issue was not reproduced")
	}

	if r.obj == nil {
		return
	}
	if len(r.obj.Status.Conditions) == 0 {
		r.log.Info("condition", "helmrelease", r.name, "type", "<none>")
	}
	for _, c := range r.obj.Status.Conditions {
		r.log.Info("condition", "helmrelease", r.name,
			"type", c.Type, "status", c.Status,
			"reason", c.Reason, "message", c.Message)
	}
	if len(r.obj.Status.History) == 0 {
		r.log.Info("history", "helmrelease", r.name, "version", "<none>")
		return
	}
	for _, h := range r.obj.Status.History {
		if h == nil {
			continue
		}
		r.log.Info("history", "helmrelease", r.name, "version", h.Version,
			"status", h.Status, "action", h.Action,
			"chart", h.VersionedChartName())
	}
}

func (r *repro) isStuck() bool {
	return r.stuck
}

func (r *repro) logStormProgress(ctx context.Context, stats []workerStat) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		prev := make([]int64, len(stats))
		last := time.Now()
		for {
			select {
			case <-ctx.Done():
				r.logWorkerStats(stats, prev, time.Since(last))
				return
			case t := <-tick.C:
				r.logWorkerStats(stats, prev, t.Sub(last))
				last = t
			}
		}
	}()
	return done
}

func (r *repro) logWorkerStats(stats []workerStat, prev []int64, dt time.Duration) {
	sec := dt.Seconds()
	if sec <= 0 {
		sec = 1
	}
	for w := range stats {
		ok := stats[w].success.Load()
		r.log.Info("patch stats",
			"helmrelease", r.name, "worker", w,
			"success", ok, "error", stats[w].error.Load(),
			"rate", int(float64(ok-prev[w])/sec))
		prev[w] = ok
	}
}

func (r *repro) awaitReconcileAck(ctx context.Context, token string) (*helmv2.HelmRelease, error) {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("reconcile request %q not acknowledged within %s", token, probeTimeout)
		case <-tick.C:
			obj, err := r.get(ctx)
			if apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("HelmRelease disappeared")
			}
			if err != nil {
				continue
			}
			if obj.Status.LastHandledReconcileAt == token {
				return obj, nil
			}
		}
	}
}

func (r *repro) get(ctx context.Context) (*helmv2.HelmRelease, error) {
	obj := &helmv2.HelmRelease{}
	err := r.client.Get(ctx, types.NamespacedName{Name: r.name, Namespace: namespace}, obj)
	return obj, err
}

func (r *repro) patchAnnotations(ctx context.Context, anns map[string]string) error {
	body, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": anns}})
	if err != nil {
		return err
	}
	return r.client.Patch(ctx, &helmv2.HelmRelease{
		ObjectMeta: metav1.ObjectMeta{Name: r.name, Namespace: namespace},
	}, client.RawPatch(types.MergePatchType, body))
}

func isStuck(obj *helmv2.HelmRelease) bool {
	ready, _ := condition(obj.Status.Conditions, meta.ReadyCondition)
	released, _ := condition(obj.Status.Conditions, helmv2.ReleasedCondition)
	latest := obj.Status.History.Latest()
	return ready != string(metav1.ConditionTrue) && released == "" && latest != nil && latest.Status == "deployed"
}

func mustClient() client.Client {
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{},
	).ClientConfig()
	if err != nil {
		fatal("load kubeconfig", err)
	}

	cfg.QPS = 20000
	cfg.Burst = 40000

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		fatal("add kubernetes scheme", err)
	}
	if err := helmv2.AddToScheme(scheme); err != nil {
		fatal("add helm-controller scheme", err)
	}
	if err := sourcev1.AddToScheme(scheme); err != nil {
		fatal("add source-controller scheme", err)
	}

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fatal("create client", err)
	}
	return c
}

func ensureNamespace(ctx context.Context, c client.Client) error {
	err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func ensureCABundle(ctx context.Context, c client.Client) error {
	if *caBundlePath == "" {
		return nil
	}
	pem, err := os.ReadFile(*caBundlePath)
	if err != nil {
		return fmt.Errorf("read CA_BUNDLE_PATH: %w", err)
	}
	slog.Info("using CA bundle", "path", *caBundlePath)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "podinfo-ca", Namespace: namespace},
		Type:       corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"ca.crt": pem,
			"caFile": pem,
		},
	}
	err = c.Create(ctx, secret)
	if apierrors.IsAlreadyExists(err) {
		existing := &corev1.Secret{}
		if err := c.Get(ctx, types.NamespacedName{Name: secret.Name, Namespace: namespace}, existing); err != nil {
			return err
		}
		existing.Data = secret.Data
		return c.Update(ctx, existing)
	}
	return err
}

func ensureHelmRepository(ctx context.Context, c client.Client) error {
	repo := &sourcev1.HelmRepository{
		ObjectMeta: metav1.ObjectMeta{Name: podinfoRepo, Namespace: namespace},
		Spec: sourcev1.HelmRepositorySpec{
			Interval: metav1.Duration{Duration: time.Hour},
			URL:      "https://stefanprodan.github.io/podinfo",
		},
	}
	if *caBundlePath != "" {
		repo.Spec.CertSecretRef = &meta.LocalObjectReference{Name: "podinfo-ca"}
	}
	err := c.Create(ctx, repo)
	if apierrors.IsAlreadyExists(err) {
		existing := &sourcev1.HelmRepository{}
		if err := c.Get(ctx, types.NamespacedName{Name: podinfoRepo, Namespace: namespace}, existing); err != nil {
			return err
		}
		existing.Spec = repo.Spec
		if err := c.Update(ctx, existing); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	key := types.NamespacedName{Name: podinfoRepo, Namespace: namespace}
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("HelmRepository podinfo not Ready within 3m")
		case <-tick.C:
			got := &sourcev1.HelmRepository{}
			if err := c.Get(ctx, key, got); err != nil {
				continue
			}
			if status, _ := condition(got.Status.Conditions, meta.ReadyCondition); status == string(metav1.ConditionTrue) {
				return nil
			}
		}
	}
}

func condition(conds []metav1.Condition, condType string) (status, message string) {
	c := apimeta.FindStatusCondition(conds, condType)
	if c == nil {
		return "", ""
	}
	return string(c.Status), c.Message
}

func fatal(msg string, err error) {
	slog.Error(msg, "error", err)
	os.Exit(1)
}
