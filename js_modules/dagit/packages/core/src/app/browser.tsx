// The Clipboard can be undefined in an insecure context.
// https://developer.mozilla.org/en-US/docs/Web/API/Clipboard_API
export const useClipboard = (): Clipboard | null => navigator.clipboard || null;
