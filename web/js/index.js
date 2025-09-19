// web/js/index.js
(function () {
  const userEl   = document.getElementById('puter-user');
  const statusEl = document.getElementById('puter-status');
  const dot      = document.getElementById('puter-dot');
  const loginBtn = document.getElementById('puter-login');

  function getSDK() {
    return (typeof window !== 'undefined') ? (window.Puter || window.puter || null) : null;
  }
  function setDot(ok, msg) {
    if (!dot) return;
    dot.classList.remove('ok', 'bad', 'warn');
    dot.classList.add(ok ? 'ok' : 'bad');
    if (statusEl) statusEl.textContent = msg || (ok ? 'sdk loaded' : 'not loaded');
  }

  async function refreshUser() {
    const SDK = getSDK();
    if (!SDK) {
      setDot(false, 'not loaded');
      if (userEl) userEl.textContent = '—';
      return;
    }
    try {
      const auth = SDK.auth || SDK;
      let user = null;
      if (auth && auth.getUser) {
        user = await auth.getUser();
      }
      if (user) {
        setDot(true, 'sdk + session');
        if (userEl) userEl.textContent = user.email || user.username || 'signed in';
      } else {
        setDot(true, 'sdk loaded (not signed in)');
        if (userEl) userEl.textContent = '—';
      }
    } catch {
      setDot(true, 'sdk loaded (not signed in)');
      if (userEl) userEl.textContent = '—';
    }
  }

  // Initial paint + a second pass after window load (to catch slower script)
  setDot(!!getSDK(), !!getSDK() ? 'sdk loaded' : 'not loaded');
  refreshUser();
  window.addEventListener('load', refreshUser);

  // Login button
  loginBtn?.addEventListener('click', async () => {
    const SDK = getSDK();
    if (!SDK) { alert('Puter SDK is not loaded.'); return; }
    try {
      const auth = SDK.auth || SDK;
      if (auth?.signInWithPopup) {
        await auth.signInWithPopup();
      } else if (auth?.signIn) {
        await auth.signIn();
      } else {
        window.open('https://puter.com/login', '_blank');
      }
      await refreshUser();
      alert('Signed in (if supported by this SDK build).');
    } catch (e) {
      console.error('Puter login error', e);
      alert('Login failed: ' + (e?.message || e));
    }
  });
})();