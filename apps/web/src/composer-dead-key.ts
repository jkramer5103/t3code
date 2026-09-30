/** Track a dead-key keystroke without rewriting pasted or existing prompt text. */
export function createComposerDeadKeyInput() {
  let pendingAt: number | null = null;
  return {
    reset() {
      pendingAt = null;
    },
    keyDown(
      event: Pick<KeyboardEvent, "key" | "ctrlKey" | "metaKey" | "altKey" | "isComposing">,
      now: number,
    ) {
      if (event.key === "Dead" && !event.ctrlKey && !event.metaKey && !event.altKey) {
        pendingAt = now;
      } else if (
        !event.isComposing &&
        !["Shift", " ", "`", "\u02cb", "\u2035", "\uff40", "Process"].includes(event.key)
      ) {
        pendingAt = null;
      }
    },
    textInput(text: string, now: number) {
      const pending = pendingAt;
      pendingAt = null;
      return pending !== null && now - pending <= 1000 && /^[\u02cb\u2035\uff40]$/.test(text)
        ? "`"
        : text;
    },
  };
}
