/* ════════════════════════════════════════════════════════════════════
   SALE LISTING — app.js
   P0: 筛选 · 列表 · 详情 · 图库 · 咨询
   P1: 排序 · 解析 title 提取字段
═══════════════════════════════════════════════════════════════════ */

const API      = '/api/v3/sale/listings';
const META_API = '/api/v3/sale/meta';
const LIMIT    = 24;
const ADVISOR  = 'https://t.me/pengqingw';

// ── State ──────────────────────────────────────────────────────────────────
let offset = 0, total = 0;
let metadataLoaded = false;
let currentListing = null, galleryIndex = 0;
let allItems = [];  // used for client-side sort

// ── Helpers ───────────────────────────────────────────────────────────────
const $      = id => document.getElementById(id);
const text   = v => (v == null) ? '' : String(v).trim();
const esc    = v => text(v).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const has    = v => !!text(v) && !['null','undefined','未知','none','[]'].includes(text(v).toLowerCase());
const fmt    = v => { const n=Number(v); return Number.isFinite(n)&&n>0 ? '$'+n.toLocaleString('en-US') : '' };
const first  = (o, keys) => keys.map(k=>o?.[k]).find(has)||'';
const fmtNum= n => Number.isFinite(n)&&n>0 ? n.toLocaleString('en-US') : '';

// Parse title like "炳发城｜4房5卫｜双拼别墅"
function parseTitle(title) {
  const parts = text(title).split(/[｜|]/).map(s=>s.trim()).filter(Boolean);
  const raw = {};
  for (const p of parts) {
    const beds = p.match(/(\d+)房/);  if (beds) raw.bedrooms = +beds[1];
    const baths = p.match(/(\d+)卫/); if (baths) raw.bathrooms = +baths[1];
    const size = p.match(/(\d+(?:\.\d+)?)\s*㎡?/); if (size) raw.size = +size[1].replace(/\.$/,'');
  }
  // Find the part most likely to be the area/location
  const location = parts[0] || '';
  return { raw, location };
}

// ── API ──────────────────────────────────────────────────────────────────
function queryParams(extra = {}) {
  const p = new URLSearchParams({ limit: LIMIT, offset, ...extra });
  const a = $('area')?.value, t = $('type')?.value, price = $('price')?.value;
  if (a) p.set('area', a);
  if (t) p.set('property_type', t);
  if (price) {
    const [mn, mx] = price.split('-');
    if (mn) p.set('min_price', mn);
    if (mx) p.set('max_price', mx);
  }
  return p;
}

async function loadMeta() {
  if (metadataLoaded) return;
  try {
    const r = await fetch(META_API, { cache:'no-store' });
    if (!r.ok) throw 0;
    const d = await r.json();
    const areas = d.areas||[], types = d.property_types||[];
    [...areas].filter(has).sort().forEach(v => {
      $('area').insertAdjacentHTML('beforeend',`<option value="${esc(v)}">${esc(v)}</option>`);
    });
    [...types].filter(has).sort().forEach(v => {
      $('type').insertAdjacentHTML('beforeend',`<option value="${esc(v)}">${esc(v)}</option>`);
    });
  } catch { /* non-critical */ }
  metadataLoaded = true;
}

async function loadList(reset = false) {
  if (reset) { offset = 0; allItems = []; }
  showSkeletons();
  setCount('读取中…');
  try {
    await loadMeta();
    const r = await fetch(API+'?'+queryParams(), { cache:'no-store' });
    if (!r.ok) throw 0;
    const d = await r.json();
    const items = Array.isArray(d.items) ? d.items : [];
    total = Number.isFinite(Number(d.total)) ? Number(d.total) : items.length;

    if (reset) allItems = items;
    else allItems.push(...items);

    applySort();
    renderList();
    renderPager();
    updateHeroImage(items);
    updateUrlFilters();

    // deep-link detail
    const id = new URLSearchParams(location.search).get('id');
    if (id && !currentListing) openDetail(id, false);
  } catch {
    setCount('');
    $('grid').innerHTML = emptyMarkup(
      '暂时无法读取出售房源',
      [{ label:'重新加载', primary:true, action:'load(true)' }, { label:'中文顾问', href:ADVISOR }]
    );
  }
}

// ── Sort (client-side) ────────────────────────────────────────────────────
function applySort() {
  const sort = $('sortSelect')?.value || 'default';
  if (sort === 'default') return; // already in natural order
  if (sort === 'price_asc')  allItems.sort((a,b)=>(a.sale_price_usd||0)-(b.sale_price_usd||0));
  if (sort === 'price_desc') allItems.sort((a,b)=>(b.sale_price_usd||0)-(a.sale_price_usd||0));
  if (sort === 'updated_desc') allItems.sort((a,b)=>new Date(b.updated_at||0)-new Date(a.updated_at||0));
}

// ── Render list ──────────────────────────────────────────────────────────
function renderList() {
  const items = allItems.slice(offset, offset + LIMIT);
  $('count').textContent = total ? `${fmtNum(total)} 套在售` : '暂无';
  $('heroCountNum').textContent = total > 0 ? fmtNum(total) : '—';
  $('grid').innerHTML = items.length
    ? items.map(item => cardMarkup(item)).join('')
    : emptyMarkup('当前条件下暂无出售房源', [
        { label:'清除筛选', primary:true, action:'clearFilters()' },
        { label:'全部房源', action:'load(true)' },
        { label:'中文顾问', href:ADVISOR }
      ]);
  updateChips();
}

function setCount(txt) { if ($('count')) $('count').textContent = txt; }

// ── Card markup ──────────────────────────────────────────────────────────
function cardMarkup(i) {
  const id     = esc(i.public_id);
  const price  = fmt(i.sale_price_usd);
  const title  = text(i.title) || '金边出售房源';
  const status = text(i.status);
  const { raw, location } = parseTitle(title);
  const type   = text(i.property_type);
  const specs  = [
    raw.bedrooms  ? `${raw.bedrooms}房`  : '',
    raw.bathrooms ? `${raw.bathrooms}卫` : '',
    raw.size      ? `${fmtNum(raw.size)}㎡` : '',
  ].filter(Boolean).join(' · ');
  const ppsm   = (raw.size && i.sale_price_usd)
    ? `约 ${fmt(Math.round(i.sale_price_usd / raw.size))}/㎡` : '';
  const photoCount = Array.isArray(i.gallery_urls) ? i.gallery_urls.length : 0;
  const imgUrl = text(i.cover_url);
  const statusClass = status==='在售'?'on-sale':status==='待确认'?'pending':'sold';
  const statusLabel = {在售:'在售',待确认:'待确认',已售:'已售',已下架:'已下架'}[status]||'';
  const telegramUrl = `${ADVISOR}?text=${encodeURIComponent(
    `你好，想咨询金边出售房源 ${id}\n${title}\n${price}`
  )}`;

  return `<article class="card">
    <button class="card-link" type="button" data-id="${id}" aria-label="查看 ${esc(title)}">
      <div class="card-image">
        ${imgUrl
          ? `<img src="${esc(imgUrl)}" alt="${esc(title)}" loading="lazy" onerror="this.style.display='none';this.nextElementSibling.style.display='grid'"><div class="image-fallback" style="display:none">暂无照片</div>`
          : `<div class="image-fallback">暂无照片</div>`}
        <div class="card-overlay"></div>
        ${statusLabel ? `<div class="card-status"><span class="status-tag ${statusClass}">${esc(statusLabel)}</span></div>` : ''}
        ${photoCount > 1 ? `<div class="card-photo-count">${photoCount} 张</div>` : ''}
        ${price ? `<div class="card-price-overlay"><span class="card-price-text">${price}</span></div>` : ''}
      </div>
      <div class="card-body">
        <h2 class="card-title">${esc(title)}</h2>
        ${type ? `<p class="card-type">${esc(type)}${location ? ' · '+esc(location) : ''}</p>` : ''}
        ${specs ? `<p class="card-specs"><span>${esc(specs)}</span></p>` : ''}
        ${ppsm   ? `<p class="card-specs"><span style="color:var(--accent)">${esc(ppsm)}</span></p>` : ''}
        <p class="card-id">编号 ${esc(id)}</p>
      </div>
    </button>
    <div class="card-actions">
      <button type="button" class="detail-btn" data-id="${id}">查看详情</button>
      <a href="${telegramUrl}" target="_blank" rel="noopener">咨询顾问</a>
    </div>
  </article>`;
}

// ── Detail ───────────────────────────────────────────────────────────────
async function openDetail(id, push = true) {
  try {
    const r = await fetch(API+'/'+encodeURIComponent(id), { cache:'no-store' });
    if (!r.ok) throw 0;
    const d = await r.json();
    currentListing = d.item || d.listing || d.data || d.result || d;
    if (!currentListing || !has(currentListing.public_id)) throw 0;
    galleryIndex = 0;
    const i = currentListing;
    const { raw, location } = parseTitle(text(i.title));
    const price = fmt(i.sale_price_usd);
    const status = text(i.status);
    const type   = text(i.property_type);
    const desc   = first(i,['description','description_text','adviser_copy','listing_description']);
    const amenities = listValue(first(i,['amenities','features','facilities']));
    const photoCount = Array.isArray(i.gallery_urls) ? i.gallery_urls.length : 0;

    // Price
    $('detail-price').textContent = price;

    // Status tag
    const statusClass = status==='在售'?'on-sale':status==='待确认'?'pending':'sold';
    const statusLabel = {在售:'在售',待确认:'待确认',已售:'已售',已下架:'已下架'}[status]||'';
    const statusEl = $('detail-status');
    if (statusLabel) {
      statusEl.textContent = statusLabel;
      statusEl.className = `detail-status-tag ${statusClass}`;
      statusEl.hidden = false;
    } else { statusEl.hidden = true; }

    // Title & meta
    $('detail-title').textContent = text(i.title) || '金边出售房源';
    $('detail-meta').textContent = [type, location].filter(Boolean).join(' · ');

    // PPSM
    const ppsmEl = $('detailPpsmBlock');
    if (raw.size && i.sale_price_usd) {
      $('detailPpsm').textContent = `${fmt(Math.round(i.sale_price_usd / raw.size))}/㎡`;
      ppsmEl.hidden = false;
    } else { ppsmEl.hidden = true; }

    // Params
    const fields = [
      ['物业类型',  type],
      ['位置',      location],
      ['卧室',      raw.bedrooms ? `${raw.bedrooms} 间` : ''],
      ['卫浴',      raw.bathrooms ? `${raw.bathrooms} 间` : ''],
      ['面积',      raw.size ? `${fmtNum(raw.size)} ㎡` : ''],
      ['楼层',      has(i.floor) ? `${i.floor} 楼` : ''],
      ['状态',      statusLabel],
      ['编号',      esc(i.public_id)],
    ].filter(([,v])=>has(v));

    $('detail-fields').innerHTML = fields.map(([l,v]) =>
      `<div class="param"><small>${esc(l)}</small><strong>${esc(v)}</strong></div>`
    ).join('');

    // Description
    const descEl = $('descriptionBlock');
    if (has(desc)) {
      $('detail-description').textContent = desc;
      descEl.hidden = false;
    } else { descEl.hidden = true; }

    // Amenities
    const amEl = $('amenitiesBlock');
    if (amenities.length) {
      $('detail-amenities').innerHTML = amenities.map(v=>`<span>${esc(v)}</span>`).join('');
      amEl.hidden = false;
    } else { amEl.hidden = true; }

    // Gallery
    paintGallery(i, photoCount);

    // Telegram context
    const telegramText = `你好，想咨询金边出售房源：\n编号：${esc(i.public_id)}\n${text(i.title)}\n${price}`;
    $('detail-telegram').href = `${ADVISOR}?text=${encodeURIComponent(telegramText)}`;
    $('detail-footer-meta').textContent = `${esc(i.public_id)} · ${text(i.title)}`.slice(0,40);

    $('overlay').hidden = false;
    document.body.style.overflow = 'hidden';
    if (push) {
      const u = new URL(location.href); u.searchParams.set('id',id);
      history.pushState({},'',u);
    }
  } catch {
    $('overlay').hidden = true;
    document.body.style.overflow = '';
    currentListing = null;
  }
}

function paintGallery(i, photoCount) {
  const gallery = $('gallery');
  const imgs = [];
  if (has(i.cover_url)) imgs.push(text(i.cover_url));
  if (Array.isArray(i.gallery_urls)) {
    i.gallery_urls.filter(has).forEach(u => { if (!imgs.includes(u)) imgs.push(u); });
  }
  const total = imgs.length;

  gallery.innerHTML = imgs.length
    ? imgs.map((u,n) => `<img src="${esc(u)}" alt="房源照片 ${n+1}" loading="${n===0?'eager':'lazy'}" style="flex:0 0 100%;width:100%;height:100%;object-fit:cover">`).join('')
    : `<div class="image-fallback" style="width:100%;flex:0 0 100%">暂无房源照片</div>`;

  gallery.style.transform = `translateX(-${galleryIndex*100}%)`;
  $('galleryCounter').textContent = total ? `${galleryIndex+1} / ${total}` : '0 / 0';
  $('galleryTotal').textContent = total;
  $('galleryPrev').hidden = total < 2;
  $('galleryNext').hidden = total < 2;
  paintDots(total);
}

function paintDots(count) {
  const dots = $('galleryDots');
  if (!dots) return;
  dots.innerHTML = Array.from({length: count}, (_,n) =>
    `<div class="gallery-dot${n===galleryIndex?' active':''}" data-index="${n}" role="presentation"></div>`
  ).join('');
  dots.querySelectorAll('.gallery-dot').forEach(d => {
    d.addEventListener('click', () => {
      galleryIndex = +d.dataset.index;
      stepGallery(0);
    });
  });
}

function stepGallery(step) {
  const imgs = getGalleryImages();
  if (imgs.length < 2) return;
  galleryIndex = (galleryIndex + step + imgs.length) % imgs.length;
  $('gallery').style.transform = `translateX(-${galleryIndex*100}%)`;
  $('galleryCounter').textContent = `${galleryIndex+1} / ${imgs.length}`;
  paintDots(imgs.length);
}

function getGalleryImages() {
  const i = currentListing;
  if (!i) return [];
  const imgs = [];
  if (has(i.cover_url)) imgs.push(text(i.cover_url));
  if (Array.isArray(i.gallery_urls)) i.gallery_urls.filter(has).forEach(u => { if (!imgs.includes(u)) imgs.push(u); });
  return imgs;
}

function closeDetail(updateUrl = true) {
  $('overlay').hidden = true;
  document.body.style.overflow = '';
  currentListing = null;
  if (updateUrl) {
    const u = new URL(location.href); u.searchParams.delete('id');
    history.pushState({},'',u);
  }
}

// ── Filters & chips ──────────────────────────────────────────────────────
function clearFilters() {
  if ($('area')) $('area').value = '';
  if ($('type')) $('type').value = '';
  if ($('price')) $('price').value = '';
  if ($('sortSelect')) $('sortSelect').value = 'default';
  loadList(true);
}

function updateChips() {
  const chips = [];
  const area = $('area')?.value;
  const type = $('type')?.value;
  const price = $('price')?.value;
  const priceLabels = { '0-80000':'$8万内','80000-120000':'$8–12万','120000-200000':'$12–20万','200000-350000':'$20–35万','350000-999999999':'$35万+' };

  if (area)  chips.push({ label: area,  onRemove: () => { if($('area')) $('area').value=''; loadList(true); }});
  if (type)  chips.push({ label: type,  onRemove: () => { if($('type')) $('type').value=''; loadList(true); }});
  if (price) chips.push({ label: priceLabels[price]||price, onRemove: () => { if($('price')) $('price').value=''; loadList(true); }});

  const row = $('chipsRow');
  const container = $('activeChips');
  if (!row || !container) return;

  if (chips.length === 0) { row.hidden = true; return; }
  row.hidden = false;
  container.innerHTML = chips.map((c,i) =>
    `<button class="chip" type="button" onclick="window._chipRemove(${i})">${esc(c.label)}<span class="chip-remove" aria-hidden="true">×</span></button>`
  ).join('');
  window._chipRemove = chips.map(c => c.onRemove);
}

function updateUrlFilters() {
  const params = new URLSearchParams();
  const area = $('area')?.value; if (area) params.set('area', area);
  const type = $('type')?.value; if (type) params.set('type', type);
  const price = $('price')?.value; if (price) params.set('price', price);
  const sort  = $('sortSelect')?.value; if (sort && sort!=='default') params.set('sort', sort);
  const search = params.toString();
  const newUrl = search ? `?${search}` : location.pathname;
  history.replaceState({},'', newUrl);
}

function restoreFiltersFromUrl() {
  const p = new URLSearchParams(location.search);
  const area  = p.get('area');  if (area  && $('area'))  $('area').value  = area;
  const type  = p.get('type');  if (type  && $('type'))  $('type').value  = type;
  const price = p.get('price'); if (price && $('price')) $('price').value = price;
  const sort  = p.get('sort');  if (sort  && $('sortSelect')) $('sortSelect').value = sort;
}

// ── Skeleton ─────────────────────────────────────────────────────────────
function showSkeletons() {
  $('grid').innerHTML = Array(6).fill('<div class="skeleton"></div>').join('');
}

function emptyMarkup(msg, actions) {
  const btns = actions.map(a =>
    a.href
      ? `<a href="${esc(a.href)}" ${a.primary?'class="primary"':''} target="_blank" rel="noopener">${esc(a.label)}</a>`
      : `<button type="button" ${a.primary?'class="primary"':''} onclick="${esc(a.action)}">${esc(a.label)}</button>`
  ).join('');
  return `<div class="empty">
    <p>${esc(msg)}</p>
    ${btns ? `<div class="empty-actions">${btns}</div>` : ''}
  </div>`;
}

// ── Pager ───────────────────────────────────────────────────────────────
function renderPager() {
  const pages = Math.max(1, Math.ceil(total / LIMIT));
  const pager = $('pager');
  if (pager) pager.hidden = total <= LIMIT;
  if ($('pageno')) $('pageno').textContent = `${Math.floor(offset/LIMIT)+1} / ${pages}`;
  if ($('prev')) $('prev').disabled = offset === 0;
  if ($('next')) $('next').disabled = offset + LIMIT >= total;
}

function updateHeroImage(items) {
  const hero = items.find(i => has(i.cover_url));
  const img  = $('heroImage');
  if (hero && img) {
    img.src = text(hero.cover_url);
    img.alt = text(hero.title);
    img.onerror = () => { img.hidden = true; };
  }
}

// ── Events ──────────────────────────────────────────────────────────────
$('resetBtn')?.addEventListener('click', clearFilters);

['area','type','price'].forEach(id => {
  $(id)?.addEventListener('change', () => loadList(true));
});

$('sortSelect')?.addEventListener('change', () => {
  applySort();
  renderList();
  updateUrlFilters();
});

$('grid')?.addEventListener('click', e => {
  const btn = e.target.closest('[data-id]');
  if (!btn) return;
  if (e.target.closest('a')) return;
  openDetail(btn.dataset.id);
});

$('closeBtn')?.addEventListener('click', closeDetail);
$('overlay')?.addEventListener('click', e => { if (e.target === $('overlay')) closeDetail(); });
$('galleryPrev')?.addEventListener('click', () => stepGallery(-1));
$('galleryNext')?.addEventListener('click', () => stepGallery(+1));

$('prev')?.addEventListener('click', () => { offset = Math.max(0, offset-LIMIT); loadList(); });
$('next')?.addEventListener('click', () => { offset += LIMIT; loadList(); });

window.addEventListener('popstate', () => {
  const id = new URLSearchParams(location.search).get('id');
  if (id) openDetail(id, false); else closeDetail(false);
});

window.addEventListener('keydown', e => {
  if (e.key === 'Escape'    && !$('overlay').hidden) closeDetail();
  if (e.key === 'ArrowLeft' && !$('overlay').hidden) stepGallery(-1);
  if (e.key === 'ArrowRight'&& !$('overlay').hidden) stepGallery(+1);
});

// Touch swipe
let touchStartX = 0;
document.addEventListener('touchstart', e => { touchStartX = e.changedTouches[0].screenX; }, { passive:true });
document.addEventListener('touchend', e => {
  if ($('overlay')?.hidden) return;
  const dx = e.changedTouches[0].screenX - touchStartX;
  if (Math.abs(dx) > 50) stepGallery(dx < 0 ? +1 : -1);
}, { passive:true });

// ── Boot ────────────────────────────────────────────────────────────────
restoreFiltersFromUrl();
loadList();
