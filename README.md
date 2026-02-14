## Babylon Data Loader
Load data for babylon.

## Prerequisites
- `go 1.24.4`

- `GNU Make 3.81`

- `mongodb` [driver for `go`](http://github.com/mongodb/mongo-go-driver)

## Building and Running
`make all` will run linters, run unit tests, and build a dist.

### Executable Artifacts
By default, executable artifacts are created in `out/` as a result of a `make build` command.

## Upgrading Go Environment

To upgrade the Go environment for this project, follow these steps:

1.  **Update `go.mod`:**
    Modify the `go.mod` file to reflect the desired Go version. For example, to upgrade to Go 1.26, change the line `go 1.24.4` to `go 1.26`.

2.  **Clean and Tidy Modules:**
    After updating `go.mod`, run the following commands to clean the module cache and tidy up dependencies:
    ```bash
    go clean -modcache
    go mod tidy
    ```

3.  **Upgrade `golangci-lint`:**
    If you encounter issues with `golangci-lint` after upgrading Go (e.g., "Go language version used to build golangci-lint is lower than the targeted Go version"), you might need to upgrade `golangci-lint`.
    If installed via Homebrew (macOS), run:
    ```bash
    brew upgrade golangci-lint
    ```
    For other installation methods, refer to the `golangci-lint` documentation.

4.  **Verify the Upgrade:**
    After performing the above steps, run `make` to ensure all linters, tests, and builds pass with the new Go version.
    ```bash
    make
    ```
