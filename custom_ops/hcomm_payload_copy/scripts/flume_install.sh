#!/usr/bin/env bash

set -u

readonly FLUME_PACKAGE_NAME="aicpu_flume_hcomm_payload.tar.gz"
readonly FLUME_DEFAULT_PACKAGE_PATH="opp/vendors/flume/aicpu/kernel"
readonly FLUME_INSTALLER_VERSION="whitelist-transaction-v1"
readonly FLUME_BEGIN_MARKER="# BEGIN FLUME MANAGED AICPU PACKAGE: ${FLUME_PACKAGE_NAME}"
readonly FLUME_END_MARKER="# END FLUME MANAGED AICPU PACKAGE: ${FLUME_PACKAGE_NAME}"

register_whitelist="n"
whitelist_path=""
package_path="${FLUME_DEFAULT_PACKAGE_PATH}"
operation="install"
base_args=()
rollback_args=("--uninstall" "--quiet")

log() {
    printf '[flume_custom_ops] %s\n' "$1"
}

fail() {
    log "[ERROR] $1"
    return 1
}

trim_value() {
    printf '%s' "$1" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'
}

entry_status() {
    awk -v target="${FLUME_PACKAGE_NAME}" -v expected_path="${package_path}" '
        function trim(value) {
            sub(/^[[:space:]]+/, "", value)
            sub(/[[:space:]]+$/, "", value)
            return value
        }
        function finish() {
            if (name != target) return
            matches++
            if (install_path == "2" && optional == "true" &&
                package_path == expected_path && load_as_per_soc == "false") {
                exact++
            } else {
                invalid++
            }
        }
        /^[[:space:]]*#/ { next }
        /^[[:space:]]*name[[:space:]]*:/ {
            finish()
            name = $0
            sub(/^[^:]*:/, "", name)
            name = trim(name)
            install_path = optional = package_path = load_as_per_soc = ""
            next
        }
        /^[[:space:]]*install_path[[:space:]]*:/ {
            install_path = $0
            sub(/^[^:]*:/, "", install_path)
            install_path = trim(install_path)
            next
        }
        /^[[:space:]]*optional[[:space:]]*:/ {
            optional = $0
            sub(/^[^:]*:/, "", optional)
            optional = trim(optional)
            next
        }
        /^[[:space:]]*package_path[[:space:]]*:/ {
            package_path = $0
            sub(/^[^:]*:/, "", package_path)
            package_path = trim(package_path)
            next
        }
        /^[[:space:]]*load_as_per_soc[[:space:]]*:/ {
            load_as_per_soc = $0
            sub(/^[^:]*:/, "", load_as_per_soc)
            load_as_per_soc = trim(load_as_per_soc)
            next
        }
        END {
            finish()
            if (matches == 0) print "missing"
            else if (matches == 1 && exact == 1 && invalid == 0) print "ready"
            else print "conflict"
        }
    ' "${whitelist_path}"
}

managed_block_status() {
    awk -v begin="${FLUME_BEGIN_MARKER}" -v end="${FLUME_END_MARKER}" \
        -v target="${FLUME_PACKAGE_NAME}" -v expected_path="${package_path}" '
        function trim(value) {
            sub(/^[[:space:]]+/, "", value)
            sub(/[[:space:]]+$/, "", value)
            return value
        }
        $0 == begin {
            begin_count++
            if (inside || closed) malformed = 1
            inside = 1
            next
        }
        $0 == end {
            end_count++
            if (!inside) malformed = 1
            inside = 0
            closed = 1
            next
        }
        inside && /^[[:space:]]*name[[:space:]]*:/ {
            name = $0; sub(/^[^:]*:/, "", name); name = trim(name); next
        }
        inside && /^[[:space:]]*install_path[[:space:]]*:/ {
            install_path = $0; sub(/^[^:]*:/, "", install_path)
            install_path = trim(install_path); next
        }
        inside && /^[[:space:]]*optional[[:space:]]*:/ {
            optional = $0; sub(/^[^:]*:/, "", optional)
            optional = trim(optional); next
        }
        inside && /^[[:space:]]*package_path[[:space:]]*:/ {
            package_path = $0; sub(/^[^:]*:/, "", package_path)
            package_path = trim(package_path); next
        }
        inside && /^[[:space:]]*load_as_per_soc[[:space:]]*:/ {
            load_as_per_soc = $0; sub(/^[^:]*:/, "", load_as_per_soc)
            load_as_per_soc = trim(load_as_per_soc); next
        }
        END {
            if (begin_count == 0 && end_count == 0) print "missing"
            else if (!malformed && !inside && begin_count == 1 &&
                     end_count == 1 && name == target &&
                     install_path == "2" && optional == "true" &&
                     package_path == expected_path &&
                     load_as_per_soc == "false") print "ready"
            else print "malformed"
        }
    ' "${whitelist_path}"
}

validate_whitelist_args() {
    if [ "${register_whitelist}" != "y" ]; then
        return 0
    fi
    case "${whitelist_path}" in
        /*) ;;
        *) fail "--flume-whitelist-path must be an absolute path"; return 1 ;;
    esac
    if [ ! -f "${whitelist_path}" ]; then
        fail "AICPU whitelist does not exist: ${whitelist_path}"
        return 1
    fi
    whitelist_path=$(realpath "${whitelist_path}") || {
        fail "cannot resolve AICPU whitelist path"
        return 1
    }
    if [ ! -r "${whitelist_path}" ] || [ ! -w "${whitelist_path}" ]; then
        fail "AICPU whitelist is not readable and writable: ${whitelist_path}"
        return 1
    fi
    if [ ! -w "$(dirname "${whitelist_path}")" ]; then
        fail "AICPU whitelist directory is not writable: $(dirname "${whitelist_path}")"
        return 1
    fi
    if [ -s "${whitelist_path}" ]; then
        local last_byte
        last_byte=$(tail -c 1 "${whitelist_path}" | od -An -t u1 | tr -d '[:space:]')
        if [ "${last_byte}" != "10" ]; then
            fail "AICPU whitelist must end with a newline for reversible editing"
            return 1
        fi
    fi
    case "${package_path}" in
        ""|/*|*..*|*[!A-Za-z0-9_./-]*)
            fail "invalid relative AICPU package path: ${package_path}"
            return 1
            ;;
    esac
    local status block_status
    status=$(entry_status) || return 1
    if [ "${status}" = "conflict" ]; then
        fail "conflicting whitelist entry exists for ${FLUME_PACKAGE_NAME}"
        return 1
    fi
    block_status=$(managed_block_status) || return 1
    if [ "${block_status}" = "malformed" ]; then
        fail "malformed Flume whitelist ownership markers; refusing to edit"
        return 1
    fi
    return 0
}

new_temp_file() {
    mktemp "${whitelist_path}.flume.XXXXXX"
}

register_package() {
    local status temp
    status=$(entry_status) || return 1
    if [ "${status}" = "ready" ]; then
        log "[INFO] AICPU package whitelist entry is already ready"
        log "aicpu_whitelist_action=preserved-existing"
        return 0
    fi
    if [ "${status}" != "missing" ]; then
        fail "refusing to replace a conflicting AICPU whitelist entry"
        return 1
    fi
    temp=$(new_temp_file) || return 1
    if ! cp -p "${whitelist_path}" "${temp}"; then
        rm -f "${temp}"
        return 1
    fi
    {
        printf '%s\n' "${FLUME_BEGIN_MARKER}"
        printf 'name:%s\n' "${FLUME_PACKAGE_NAME}"
        printf 'install_path:2\n'
        printf 'optional:true\n'
        printf 'package_path:%s\n' "${package_path}"
        printf 'load_as_per_soc:false\n'
        printf '%s\n' "${FLUME_END_MARKER}"
    } >> "${temp}"
    if ! mv "${temp}" "${whitelist_path}"; then
        rm -f "${temp}"
        return 1
    fi
    log "[INFO] Registered ${FLUME_PACKAGE_NAME} in ${whitelist_path}"
    log "aicpu_whitelist_action=created-managed"
}

unregister_package() {
    local block_status temp
    block_status=$(managed_block_status) || return 1
    if [ "${block_status}" = "missing" ]; then
        log "[INFO] No Flume-managed whitelist entry to remove"
        log "aicpu_whitelist_action=preserved-external-or-missing"
        return 0
    fi
    if [ "${block_status}" != "ready" ]; then
        fail "malformed Flume whitelist ownership markers; refusing to edit"
        return 1
    fi
    temp=$(new_temp_file) || return 1
    if ! cp -p "${whitelist_path}" "${temp}"; then
        rm -f "${temp}"
        return 1
    fi
    if ! awk -v begin="${FLUME_BEGIN_MARKER}" -v end="${FLUME_END_MARKER}" '
        $0 == begin { inside = 1; next }
        $0 == end { inside = 0; next }
        !inside { print }
        END { if (inside) exit 1 }
    ' "${whitelist_path}" > "${temp}"; then
        rm -f "${temp}"
        return 1
    fi
    if ! mv "${temp}" "${whitelist_path}"; then
        rm -f "${temp}"
        return 1
    fi
    log "[INFO] Removed Flume-managed whitelist entry from ${whitelist_path}"
    log "aicpu_whitelist_action=removed-managed"
}

for arg in "$@"; do
    case "${arg}" in
        --flume-register-whitelist)
            register_whitelist="y"
            ;;
        --flume-whitelist-path=*)
            whitelist_path=$(trim_value "${arg#*=}")
            ;;
        --flume-whitelist-package-path=*)
            package_path=$(trim_value "${arg#*=}")
            ;;
        --uninstall)
            operation="uninstall"
            base_args+=("${arg}")
            ;;
        --install-path=*)
            base_args+=("${arg}")
            rollback_args+=("${arg}")
            ;;
        *)
            base_args+=("${arg}")
            ;;
    esac
done

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
readonly script_dir
base_installer="${script_dir}/flume_base_install.sh"
readonly base_installer
if [ ! -x "${base_installer}" ]; then
    fail "CANN base installer is missing or not executable: ${base_installer}"
    exit 1
fi
if ! validate_whitelist_args; then
    exit 1
fi
log "flume_installer=${FLUME_INSTALLER_VERSION}"

if [ "${operation}" = "uninstall" ]; then
    if ! "${base_installer}" "${base_args[@]}"; then
        exit 1
    fi
    if [ "${register_whitelist}" = "y" ] && ! unregister_package; then
        exit 1
    fi
    exit 0
fi

if ! "${base_installer}" "${base_args[@]}"; then
    exit 1
fi
if [ "${register_whitelist}" = "y" ] && ! register_package; then
    log "[ERROR] Whitelist registration failed; rolling back package files"
    log "aicpu_package_rollback=attempted"
    "${base_installer}" "${rollback_args[@]}" || \
        log "[ERROR] Package rollback also failed; manual cleanup is required"
    exit 1
fi
exit 0
