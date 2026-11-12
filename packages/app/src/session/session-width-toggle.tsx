import { Icon } from "@opencode/ui/icon"
import { IconButton } from "@opencode/ui/icon-button"
import { Tooltip } from "@opencode/ui/tooltip"
import { useLanguage } from "@/runtime/i18n/language"
import { useSessionLayout } from "@/session/session-layout"

export function SessionWidthToggle() {
  const language = useLanguage()
  const { view } = useSessionLayout()
  const label = () => language.t(view().session.wide() ? "session.width.narrow" : "session.width.wide")

  return (
    <Tooltip placement="top" value={label()}>
      <IconButton
        type="button"
        variant="ghost-muted"
        size="small"
        data-action="session-width-toggle"
        aria-label={label()}
        aria-pressed={view().session.wide()}
        onClick={() => view().session.toggleWide()}
        icon={<Icon name={view().session.wide() ? "collapse" : "expand"} size="small" class="rotate-90" />}
      />
    </Tooltip>
  )
}
