# Fork setup

This document explains which values in this repository depend on the
owner of the repository (GitHub or Docker Hub), how the workflows in
`.github/workflows/` resolve them, and what a fork owner has to configure so
that their fork publishes under their own namespace.

## What is automatic

The workflows do not hard-code the repository owner. They derive it from
GitHub's own context:

* `github.repository_owner` is the GitHub username or organization that
  owns the repository in which the workflow is running. In a fork that is
  the fork's owner, never the upstream's.
* `github.repository` is `<owner>/<repo>`, e.g. `VulcanoSoftware/tgdcfs`.

Workflows use these like this:

| Workflow | What is derived |
|---|---|
| `docker-publish.yml` | Docker Hub namespace for the `tgdcfs` and `tgdcfs-fe` images |
| `docker-preview.yml` | Same namespace, for `preview-<commit>` and `preview` tags |
| `release.yml` | Same namespace, for `X.Y.Z` and `X.Y` release tags |
| `test.yml` | Codecov `slug` for the coverage upload |

A fork that runs these workflows therefore publishes to
`<fork-owner>/tgdcfs` and `<fork-owner>/tgdcfs-fe`, not to the upstream
namespace.

## What a fork owner must configure

Docker Hub does not know about GitHub forks. A fork that wants its images
published under its own Docker Hub account needs credentials that GitHub
Actions can use, and the repositories `tgdcfs` and `tgdcfs-fe` must exist
on that Docker Hub account.

### 1. Create the Docker Hub repositories

Log into [hub.docker.com](https://hub.docker.com/) and create the public
repositories:

* `<dockerhub-username>/tgdcfs`
* `<dockerhub-username>/tgdcfs-fe`

They must be named exactly like that; the workflows derive the image name
from the repository name in `pyproject.toml` / the matrix.

### 2. Create a Docker Hub access token

On the same Docker Hub account, open **Account Settings → Personal access
tokens → Generate new token**. Give it `Read & Write` permissions, name it
e.g. `github-actions-tgdcfs`, and copy the value once — Docker Hub shows it
only once.

### 3. Add the secrets to the fork

On the fork, go to **Settings → Secrets and variables → Actions → New
repository secret** and add:

| Name | Value |
|---|---|
| `DOCKERHUB_USERNAME` | The Docker Hub username (not the token name) |
| `DOCKERHUB_TOKEN` | The access token created above |

### 4. Optional: set `DOCKERHUB_NAMESPACE`

By default the workflows use `github.repository_owner` as the Docker Hub
namespace. If the Docker Hub namespace differs from the GitHub username,
add a repository variable instead of editing the workflows:

**Settings → Secrets and variables → Actions → Variables tab → New
repository variable**

| Name | Value |
|---|---|
| `DOCKERHUB_NAMESPACE` | The Docker Hub namespace, e.g. `mydockerhuborg` |

Every workflow that pushes images reads `vars.DOCKERHUB_NAMESPACE` with a
fallback to `github.repository_owner`:

```yaml
DOCKERHUB_NAMESPACE: ${{ vars.DOCKERHUB_NAMESPACE || github.repository_owner }}
```

Set it once and every future merge picks it up automatically.

### 5. Optional: Codecov

`test.yml` uploads coverage to Codecov with `slug: ${{ github.repository }}`,
which is always correct for the fork. If you want coverage to appear on
your Codecov dashboard, add a `CODECOV_TOKEN` repository secret. Without
it, coverage uploads are skipped on branches where they are not required
(`pull_request` and `master`).

## What is deliberately not automatic

* **GitHub Pages** (`gh-pages.yml`) is bound to the `tgdcfs-gh-pages/`
  Next.js site, and the Docusaurus site under `docusaurus/` hard-codes
  `organizationName`/`url` to `Xyvran` because that is where the published
  documentation lives. A fork that wants its own Pages site has to change
  those values in `docusaurus/docusaurus.config.ts` and the `url` in
  `gh-pages.yml`. Upstream values are kept because the published
  documentation is a single source of truth for the project.
* **Badges in `README.md`** point at the upstream repository because that
  is the source of truth for the project's CI status. A fork that wants
  its own badges has to change the `<owner>` in the badge URLs in the first
  lines of `README.md`.

## Guard against accidental upstream publication

Workflows never reference a hard-coded upstream namespace, so a fork cannot
publish to it by accident: `github.repository_owner` is always the fork's
owner. The only way a fork can publish to the upstream Docker Hub
namespace is by setting `DOCKERHUB_NAMESPACE` to it, which requires write
access to that namespace's Docker Hub credentials and is not something a
regular fork does.

## Summary

| What | Value | Where it comes from |
|---|---|---|
| GitHub owner | `github.repository_owner` | Automatic, per repository |
| Docker Hub namespace | `vars.DOCKERHUB_NAMESPACE \|\| github.repository_owner` | Repository variable, default GitHub owner |
| Docker Hub credentials | `secrets.DOCKERHUB_USERNAME`, `secrets.DOCKERHUB_TOKEN` | Repository secrets, per fork |
| Codecov project | `github.repository` | Automatic |
| Pages / Docusaurus | `Xyvran` | Hard-coded on purpose, single source of truth |
