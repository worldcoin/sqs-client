// Package localstacktest spins up Localstack for tests using dockertest.
package localstacktest

import (
    "fmt"
    "log/slog"
    "net/http"
    "os"
    "time"

    "github.com/ory/dockertest/v3"
    "github.com/ory/dockertest/v3/docker"
)

// NewCloudProviderWebService starts a Localstack container exposing the given service (e.g., "sqs").
// Returns the dynamic host port for 4566/tcp and a cleanup function.
func NewCloudProviderWebService(service string) (string, func(), error) {
    // Access keys are needed by AWS SDK to configure credentials. Not validated by Localstack.
    if err := os.Setenv("AWS_ACCESS_KEY_ID", "localstack"); err != nil {
        return "", nil, fmt.Errorf("cannot set aws access key id: %w", err)
    }
    if err := os.Setenv("AWS_SECRET_ACCESS_KEY", "localstack"); err != nil {
        return "", nil, fmt.Errorf("cannot set aws secret access key: %w", err)
    }

    pool, err := dockertest.NewPool("")
    if err != nil {
        return "", nil, fmt.Errorf("cannot create dockertest pool: %w", err)
    }
    if err := pool.Client.Ping(); err != nil {
        return "", nil, fmt.Errorf("cannot connect to Docker: %w", err)
    }

    resource, err := pool.RunWithOptions(&dockertest.RunOptions{
        Repository: "localstack/localstack",
        Tag:        "3.5.0",
        Env: []string{
            fmt.Sprintf("SERVICES=%s", service),
        },
    }, func(config *docker.HostConfig) {
        config.RestartPolicy = docker.AlwaysRestart()
    })
    if err != nil {
        return "", nil, fmt.Errorf("failed to run dockertest resource pool: %w", err)
    }

    if err := resource.Expire(60); err != nil {
        return "", nil, fmt.Errorf("setting resource expiration failed: %w", err)
    }

    purge := func() { _ = pool.Purge(resource) }
    port := resource.GetPort("4566/tcp")

    pool.MaxWait = 1 * time.Minute
    if err := pool.Retry(func() error {
        resp, err := http.Get(fmt.Sprintf("http://localhost:%s/_localstack/health", port))
        if err != nil {
            return err
        }
        defer func() {
            if cerr := resp.Body.Close(); cerr != nil {
                slog.Error(fmt.Sprintf("cannot close healthcheck response body: %v", cerr))
            }
        }()
        if resp.StatusCode != http.StatusOK {
            return fmt.Errorf("got status code: %d", resp.StatusCode)
        }
        return nil
    }); err != nil {
        return "", nil, fmt.Errorf("localstack is not reachable: %w", err)
    }

    return port, purge, nil
}

