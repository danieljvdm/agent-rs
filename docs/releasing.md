# Releasing

Releases are driven by change files and published by CI. Nobody runs `cargo publish` by hand.

## In a pull request that changes the crate

Add a change file to `.changeset/`, named anything, ending in `.md`:

```markdown
---
default: minor
---

# Add `Host::wait`

`Host::wait(run_id)` resolves when the run settles, so callers no longer poll `status`.
```

The heading becomes the changelog entry and the body its details. `knope document-change` writes
the file for you if you have [Knope](https://knope.tech) installed. A pull request that does not
change what users of the crate get (CI, docs in the repository, the application) needs no change
file.

While the version is `0.x`, the bump types mean:

| Type | Use for | 0.1.0 becomes |
| --- | --- | --- |
| `major` | a breaking change | 0.2.0 |
| `minor` | a new feature | 0.1.1 |
| `patch` | a fix | 0.1.1 |

## What CI does

`.github/workflows/release.yml` runs on every push to `main`.

1. **Pending change files:** it opens, or updates, a pull request from the `release` branch. That
   pull request bumps the version in `Cargo.toml` and `Cargo.lock`, moves the change files into
   `CHANGELOG.md`, and deletes them.
2. **The release pull request is merged:** the version in `Cargo.toml` now has no tag, so the
   workflow publishes the crate to crates.io, tags the commit `v<version>` and creates the GitHub
   release from the changelog entry.

The `release-config` job in `ci.yml` checks `knope.toml` and any pending change files on every
pull request.

## Credentials

There is no crates.io token in the repository. Publishing uses
[trusted publishing](https://crates.io/docs/trusted-publishing): crates.io accepts the `publish`
job because the crate's settings name this repository and `release.yml`.

The release pull request is opened with the workflow's own token, and GitHub does not start other
workflows for pull requests opened that way, so CI does not run on it. To change that, add a
personal access token with write access to contents and pull requests as the `RELEASE_TOKEN`
secret.

## If a release fails half-way

Re-run the failed `publish` job. It skips publishing when crates.io already has the version, then
tags and creates the GitHub release.
