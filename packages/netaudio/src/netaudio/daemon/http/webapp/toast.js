const TOAST_LIFETIME_MILLISECONDS = 7000;

function stack() {
  let node = document.getElementById("toast-stack");
  if (!node) {
    node = document.createElement("div");
    node.id = "toast-stack";
    node.className = "toast-stack";
    document.body.appendChild(node);
  }
  return node;
}

export function showToast(message, kind = "info") {
  const node = document.createElement("div");
  node.className = `toast ${kind}`;
  node.textContent = message;
  stack().appendChild(node);
  setTimeout(() => node.remove(), TOAST_LIFETIME_MILLISECONDS);
}

export async function runAction(description, action) {
  try {
    const result = await action();
    showToast(`${description}: ok`, "success");
    return { ok: true, result };
  } catch (error) {
    showToast(`${description}: ${error.message}`, "error");
    return { ok: false, error };
  }
}
