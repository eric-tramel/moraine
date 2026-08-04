#!/bin/sh
set -eu

moraine_bin=""
old_ifs="$IFS"
IFS=:
for path_dir in $PATH; do
  case "$path_dir" in
    /*)
      candidate="$path_dir/moraine"
      ;;
    *)
      candidate="${path_dir:-.}/moraine"
      if [ -x "$candidate" ] && [ ! -d "$candidate" ]; then
        IFS="$old_ifs"
        {
          printf '%s\n' 'moraine plugin launch error: binary_untrusted'
          printf '%s\n' 'Refusing to run a moraine executable resolved from a relative PATH entry.'
          printf '%s\n' 'Restart your agent harness from a trusted shell where moraine resolves to an absolute installed path.'
        } >&2
        exit 126
      fi
      continue
      ;;
  esac

  if [ -x "$candidate" ] && [ ! -d "$candidate" ]; then
    moraine_bin="$candidate"
    break
  fi
done
IFS="$old_ifs"

if [ -z "$moraine_bin" ]; then
  {
    printf '%s\n' 'moraine plugin launch error: binary_missing'
    printf '%s\n' 'The Moraine plugin requires the moraine CLI on PATH.'
    printf '%s\n' 'Install: uv tool install moraine-cli'
    printf '%s\n' 'Upgrade: uv tool upgrade moraine-cli'
    printf '%s\n' 'Start the stack before using MCP search: moraine up'
    printf '%s\n' 'If Moraine is already installed, restart your agent harness with its bin directory on PATH.'
  } >&2
  exit 127
fi

cwd="$(pwd -P 2>/dev/null || pwd)"
moraine_name="${moraine_bin##*/}"
moraine_parent="${moraine_bin%/*}"
moraine_dir="$(CDPATH= cd -- "$moraine_parent" 2>/dev/null && pwd -P || printf '%s\n' "$moraine_parent")"
moraine_entry="$moraine_dir/$moraine_name"

trusted_user_bin=""
trusted_uv_tools=""
if [ -n "${HOME:-}" ]; then
  trusted_user_bin="$(CDPATH= cd -- "$HOME/.local/bin" 2>/dev/null && pwd -P || :)"
  trusted_uv_tools="$(CDPATH= cd -- "$HOME/.local/share/uv/tools" 2>/dev/null && pwd -P || :)"
fi

reject_symlink() {
  {
    printf '%s\n' 'moraine plugin launch error: binary_untrusted'
    printf '%s\n' 'Refusing to run a moraine executable through an unsafe symlink chain.'
    printf '%s\n' 'Restart your agent harness with the installed moraine CLI earlier on a trusted PATH.'
  } >&2
  exit 126
}
if [ -x /usr/bin/readlink ]; then
  readlink_bin="/usr/bin/readlink"
elif [ -x /bin/readlink ]; then
  readlink_bin="/bin/readlink"
else
  reject_symlink
fi


moraine_bin="$moraine_entry"
moraine_was_symlink=0
symlink_depth=0
while [ -L "$moraine_bin" ]; do
  moraine_was_symlink=1
  symlink_depth=$((symlink_depth + 1))
  if [ "$symlink_depth" -gt 40 ]; then
    reject_symlink
  fi
  link_target="$("$readlink_bin" "$moraine_bin" 2>/dev/null)" || reject_symlink
  case "$link_target" in
    /*) ;;
    *) link_target="${moraine_bin%/*}/$link_target" ;;
  esac
  target_name="${link_target##*/}"
  target_parent="${link_target%/*}"
  target_dir="$(CDPATH= cd -- "$target_parent" 2>/dev/null && pwd -P)" || reject_symlink
  moraine_bin="$target_dir/$target_name"
done

scan_git_ancestors() {
  scan_dir="$1"
  while [ "$scan_dir" != "/" ]; do
    if [ -e "$scan_dir/.git" ]; then
      {
        printf '%s\n' 'moraine plugin launch error: binary_untrusted'
        printf '%s\n' 'Refusing to run a moraine executable from a Git worktree.'
        printf '%s\n' 'Restart your agent harness with the installed moraine CLI earlier on a trusted PATH.'
      } >&2
      exit 126
    fi
    scan_dir="${scan_dir%/*}"
    if [ -z "$scan_dir" ]; then
      scan_dir="/"
    fi
  done
}

scan_git_ancestors "$moraine_dir"
resolved_dir="${moraine_bin%/*}"
if [ "$resolved_dir" != "$moraine_dir" ]; then
  scan_git_ancestors "$resolved_dir"
fi

trusted_user_entry=0
if [ "$moraine_dir" = "$trusted_user_bin" ]; then
  if [ "$moraine_was_symlink" -eq 0 ]; then
    trusted_user_entry=1
  elif [ -n "$trusted_uv_tools" ]; then
    case "$moraine_bin" in
      "$trusted_uv_tools"/*) trusted_user_entry=1 ;;
    esac
  fi
fi

project_local=0
case "$moraine_entry" in
  "$cwd"/*) project_local=1 ;;
esac
case "$moraine_bin" in
  "$cwd"/*) project_local=1 ;;
esac
if [ "$project_local" -eq 1 ] && [ "$trusted_user_entry" -ne 1 ]; then
  {
    printf '%s\n' 'moraine plugin launch error: binary_untrusted'
    printf '%s\n' 'Refusing to run a moraine executable from the current project directory.'
    printf '%s\n' 'Restart your agent harness with the installed moraine CLI earlier on a trusted PATH.'
  } >&2
  exit 126
fi

exec "$moraine_bin" run mcp
