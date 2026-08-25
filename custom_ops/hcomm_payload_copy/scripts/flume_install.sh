#!/usr/bin/env bash

set -u

# Stage 3B.3G is intentionally parked. CANN makeself validates arguments before
# invoking this script, so the whitelist transaction flags cannot be delivered
# portably. Fail closed here as well: a standard .run --install/--uninstall must
# never leave a package-only or whitelist-only system state.
printf '%s\n' '[flume_custom_ops] flume_installer=parked-todo-v1'
printf '%s\n' '[flume_custom_ops] status=TODO'
printf '%s\n' '[flume_custom_ops] stage3b3g_system_install=parked'
printf '%s\n' '[flume_custom_ops] reason=CANN makeself cannot forward the required Flume whitelist arguments'
printf '%s\n' '[flume_custom_ops] action=none'
printf '%s\n' '[flume_custom_ops] system_changes=none'
printf '%s\n' '[flume_custom_ops] fallback=hccl-p2p'
exit 2
