export async function runAction(description, action) {
  try {
    return { ok: true, result: await action() };
  } catch (error) {
    return { ok: false, error: new Error(`${description}: ${error.message}`) };
  }
}
