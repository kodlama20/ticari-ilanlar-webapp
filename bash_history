 1192  mkdir -p tools\ncat > tools/release <<'BASH'\n#!/usr/bin/env bash\nset -euo pipefail\nREPO_ROOT="$(git rev-parse --show-toplevel)"\ncd "$REPO_ROOT"\nVER_FILE="VERSION"\nread_version(){ [[ -f "$VER_FILE" ]] && tr -d ' \n' < "$VER_FILE" || echo "0.0.0"; }\nwrite_version(){ echo -n "$1" > "$VER_FILE"; }\nbump_semver(){ IFS=. read -r MA MI PA <<<"$1"; case "$2" in major) MA=$((MA+1)); MI=0; PA=0 ;; minor) MI=$((MI+1)); PA=0 ;; patch) PA=$((PA+1)) ;; *) echo "Unknown bump: $2" >&2; exit 1;; esac; echo "${MA}.${MI}.${PA}"; }\nensure_clean_tree(){ if ! git diff --quiet || ! git diff --cached --quiet; then echo "Working tree not clean. Commit or stash first." >&2; exit 1; fi; }\nopen_pr_for_version(){ local new="$1" branch="release/v${new}"; git switch -c "$branch"; write_version "$new"; git add "$VER_FILE"; git commit -m "chore(release): bump version to v${new}"; git push -u origin "$branch"; gh pr create --title "release: v${new}" --body "Bump version to v${new}.\n\nMerge this, then run: \`tools/release tag\`"; echo "✅ Opened PR for v${new}. After merge, run: tools/release tag"; }\ncmd="${1:-}"\ncase "$cmd" in\n  major|minor|patch) ensure_clean_tree; cur="$(read_version)"; new="$(bump_semver "$cur" "$cmd")"; open_pr_for_version "$new" ;;\n  set) ensure_clean_tree; new="${2:?Usage: tools/release set X.Y.Z}"; [[ "$new" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "Invalid version: $new"; exit 1; }; open_pr_for_version "$new" ;;\n  tag) git checkout main; git pull --rebase origin main; ver="$(read_version)"; tag="v${ver}"; git rev-parse "$tag" >/dev/null 2>&1 && { echo "Tag $tag already exists." >&2; exit 1; }; git tag -a "$tag" -m "Release $tag"; git push origin "$tag"; gh release create "$tag" --generate-notes -t "$tag"; echo "🎉 Release $tag created." ;;\n  *) cat <<USAGE\nUsage:\n  tools/release patch|minor|major   # bump version, open PR\n  tools/release set X.Y.Z           # set exact version, open PR\n  tools/release tag                 # tag current main as v\$(cat VERSION) + create GitHub Release\nUSAGE\n     exit 1 ;;\nesac\nBASH\nchmod +x tools/release
 1193  git status
 1194  git checkout -b chore/add-release-helper
 1195  git add VERSION tools/release
 1196  git commit -m "chore: add VERSION file and tools/release helper"
 1197  git push -u origin chore/add-release-helper
 1198  gh pr create --fill --web
 1199  printf "0.1.0\n" > VERSION\ngit add VERSION\ngit commit --amend --no-edit\ngit push -f origin chore/add-release-helper
 1200  git config --global user.name "Alper Ozkan"\ngit config --global user.email "20kodlama@gmail.com"
 1201  bash -n tools/release
 1202  git checkout main
 1203  git pull --rebase origin main
 1204  ./tools/release patch
 1205  ./tools/release tag
 1206  tree 
 1207  history
