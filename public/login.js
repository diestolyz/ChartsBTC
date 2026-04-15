const params = new URLSearchParams(typeof location !== "undefined" ? location.search : "");
const nextPath = params.get("next");
const nextUrl =
  nextPath && nextPath.startsWith("/") && !nextPath.startsWith("//") ? nextPath : "/";

async function redirectIfAlreadyAuthed() {
  try {
    const r = await fetch("/api/auth/status", { credentials: "same-origin" });
    const j = await r.json().catch(() => ({}));
    if (j.authEnabled && j.authenticated) {
      location.replace(nextUrl);
    }
  } catch {
    /* 网络错误时留在登录页 */
  }
}

void redirectIfAlreadyAuthed();

const form = document.getElementById("login_form");
const hint = document.getElementById("login_hint");
const submitBtn = document.getElementById("login_submit");

form?.addEventListener("submit", async (e) => {
  e.preventDefault();
  if (!hint || !submitBtn) return;
  hint.hidden = true;
  hint.classList.remove("ids-panel__hint--err");
  const usernameEl = document.getElementById("login_username");
  const passwordEl = document.getElementById("login_password");
  const username = usernameEl && "value" in usernameEl ? String(usernameEl.value).trim() : "";
  const password = passwordEl && "value" in passwordEl ? String(passwordEl.value) : "";
  submitBtn.disabled = true;
  try {
    const res = await fetch("/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      credentials: "same-origin",
      body: JSON.stringify({ username, password }),
    });
    const j = await res.json().catch(() => ({}));
    if (!res.ok) {
      hint.hidden = false;
      hint.classList.add("ids-panel__hint--err");
      if (j.error === "login_disabled") {
        hint.textContent = j.hint || "服务器未启用登录（请配置 LOGIN_USERNAME 与 LOGIN_SECRET）";
      } else {
        hint.textContent = "手机号或密码错误";
      }
      return;
    }
    location.href = nextUrl;
  } catch (err) {
    hint.hidden = false;
    hint.classList.add("ids-panel__hint--err");
    hint.textContent = err instanceof Error ? err.message : String(err);
  } finally {
    submitBtn.disabled = false;
  }
});
