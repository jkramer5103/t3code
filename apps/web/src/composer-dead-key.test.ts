import { describe, expect, it } from "vite-plus/test";

import { createComposerDeadKeyInput } from "./composer-dead-key";

const key = (value: string, isComposing = false) => ({
  key: value,
  ctrlKey: false,
  metaKey: false,
  altKey: false,
  isComposing,
});

describe("composer dead-key input", () => {
  it.each(["\u02cb", "\u2035", "\uff40"])("normalizes a committed dead-key grave %s", (grave) => {
    const input = createComposerDeadKeyInput();
    input.keyDown(key("Dead"), 0);
    input.keyDown(key(" ", true), 20);
    expect(input.textInput(grave, 30)).toBe("`");
    expect(input.textInput(grave, 40)).toBe(grave);
  });

  it("preserves legitimate text and non-grave compositions", () => {
    const input = createComposerDeadKeyInput();
    expect(input.textInput("\u02cb", 0)).toBe("\u02cb");
    input.keyDown(key("Dead"), 10);
    expect(input.textInput("à", 20)).toBe("à");
    expect(input.textInput("\u02cb", 30)).toBe("\u02cb");
    input.keyDown(key("Dead"), 40);
    expect(input.textInput("\u02cbword\u02cb", 50)).toBe("\u02cbword\u02cb");
  });

  it("discards stale or interrupted dead keys", () => {
    const input = createComposerDeadKeyInput();
    input.keyDown(key("Dead"), 0);
    expect(input.textInput("\u02cb", 1001)).toBe("\u02cb");
    input.keyDown(key("Dead"), 2000);
    input.keyDown(key("ArrowLeft"), 2010);
    expect(input.textInput("\u02cb", 2020)).toBe("\u02cb");
    input.keyDown(key("Dead"), 3000);
    input.reset(); // Paste or focus loss ends the keystroke sequence.
    expect(input.textInput("\u02cb", 3010)).toBe("\u02cb");
  });

  it("does not arm for keyboard shortcuts", () => {
    const input = createComposerDeadKeyInput();
    input.keyDown({ ...key("Dead"), ctrlKey: true }, 0);
    expect(input.textInput("\u02cb", 10)).toBe("\u02cb");
  });
});
