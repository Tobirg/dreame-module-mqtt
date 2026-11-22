// MQTT module panel, mounted by the Dreame Local web UI: mount(element, ctx).
// Settings are saved by the module's "configure" action; the background service picks them up by itself.
export default function mount(el, ctx) {
  el.innerHTML = `<p data-r="status" role="status">Статус ещё не получен</p>
<label class="check"><input data-r="enabled" type="checkbox">Публиковать робот в MQTT</label>
<label>Адрес брокера<input data-r="host" autocomplete="off" placeholder="192.168.2.10"></label>
<label>Порт<input data-r="port" type="number" min="1" max="65535" value="1883"></label>
<label>Пользователь<input data-r="username" autocomplete="off"></label>
<label>Пароль<input data-r="password" type="password" autocomplete="new-password"></label>
<details><summary>Топики</summary>
<label>Базовый топик<input data-r="base_topic" autocomplete="off" placeholder="dreame_local/&lt;id робота&gt;"></label>
<label>Префикс обнаружения Home Assistant<input data-r="discovery_prefix" autocomplete="off" placeholder="homeassistant"></label></details>
<div class="row"><button data-r="save" class="primary">Сохранить</button><button data-r="test">Сохранить и проверить</button></div>
<details data-r="card"><summary>Карточка карты для Home Assistant</summary>
<p class="muted small">Для карточки xiaomi-vacuum-map-card (HACS): зоны, точки и комнаты робота. В Home Assistant: панель → «Изменить» → «Добавить карточку» → «Вручную», вставить текст. Если сущности пылесоса или камеры карты переименованы, поправьте их в первых строках.</p>
<textarea data-r="yaml" rows="12" readonly spellcheck="false"></textarea>
<div class="row"><button data-r="copy">Скопировать</button></div></details>
<p class="muted small">Подойдёт тот же брокер, что у Zigbee2MQTT. Home Assistant найдёт пылесос сам, если в нём настроена интеграция MQTT. Пароль хранится на роботе и сюда не возвращается.</p>`;
  const $ = name => el.querySelector(`[data-r="${name}"]`);
  const fields = ['enabled', 'host', 'port', 'username', 'base_topic', 'discovery_prefix'];
  let loaded = false, hasPassword = false;

  const states = { connected: 'Подключён к брокеру', connecting: 'Подключение к брокеру…', error: 'Ошибка подключения', not_configured: 'Не настроен' };
  function render(s) {
    const c = s.connection || {};
    $('status').textContent = (states[c.state] || 'Служба ещё не запустилась') + (c.broker ? ' · ' + c.broker : '') + (c.error ? ' · ' + c.error : '');
    hasPassword = s.has_password;
    $('password').placeholder = hasPassword ? 'сохранён — оставь пустым, чтобы не менять' : '';
    if (loaded) return;
    loaded = true;
    for (const f of fields) {
      if (f === 'enabled') $(f).checked = !!s.enabled;
      else $(f).value = s[f] ?? '';
    }
  }
  async function refresh() {
    try { render(await ctx.read('status')); } catch (e) { $('status').textContent = e.message; }
  }
  async function save() {
    const settings = { enabled: $('enabled').checked, host: $('host').value.trim(), port: Number($('port').value) || 1883,
      username: $('username').value.trim(), password: $('password').value, base_topic: $('base_topic').value.trim(),
      discovery_prefix: $('discovery_prefix').value.trim(), keep_password: hasPassword && $('password').value === '' };
    const s = await ctx.call('configure', settings);
    $('password').value = '';
    render(s);
  }
  $('save').onclick = async () => {
    try { await save(); ctx.toast('Настройки MQTT сохранены'); setTimeout(refresh, 3000); } catch (e) { ctx.toast(e.message, true); }
  };
  $('test').onclick = async () => {
    try { await save(); await ctx.call('test'); ctx.toast('Брокер принял подключение'); setTimeout(refresh, 3000); } catch (e) { ctx.toast(e.message, true); }
  };
  $('card').ontoggle = async () => {
    if (!$('card').open) return;
    $('yaml').value = 'Загрузка…';
    try { $('yaml').value = (await ctx.read('card')).yaml; } catch (e) { $('yaml').value = ''; ctx.toast(e.message, true); }
  };
  $('copy').onclick = async () => {
    try { await navigator.clipboard.writeText($('yaml').value); ctx.toast('Скопировано'); }
    catch { $('yaml').select(); ctx.toast('Выделено — скопируйте Ctrl+C'); }
  };
  refresh();
  setInterval(() => { if (!document.hidden) refresh(); }, 15000);
}
