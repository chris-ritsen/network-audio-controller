import { t } from "./i18n.js";

export async function runAction(description, action) {
  try {
    return { ok: true, result: await action() };
  } catch (error) {
    return { ok: false, error: new Error(t("{action}: {reason}", { action: description, reason: t(error.message) })) };
  }
}
