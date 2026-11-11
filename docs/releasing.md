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

Release pull requests use a dedicated GitHub App installation token for both the Git push and
Knope's pull request API calls. This lets the normal CI workflows start automatically. Pull
requests created with the built-in `GITHUB_TOKEN` instead require a maintainer to approve their
workflow runs, even when the release workflow separately dispatches the required checks.

Before enabling release pull request automation:

1. Register a GitHub App with repository **Contents: Read and write** and **Pull requests: Read
   and write** permissions. It does not need a webhook or organization permissions.
2. Install the app on this repository only.
3. Add its client ID as the Actions repository variable `RELEASE_APP_CLIENT_ID` and its private
   key as the Actions repository secret `RELEASE_APP_PRIVATE_KEY`.

The workflow requests a token scoped to this repository and those two permissions. The app
token is revoked when the release pull request job finishes; publishing continues to use
`GITHUB_TOKEN` and crates.io trusted publishing.

## If a release fails half-way

Re-run the failed `publish` job. It skips publishing when crates.io already has the version, then
tags and creates the GitHub release.
