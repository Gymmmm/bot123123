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
let metadataLoaded = false, initialUrlRestored = false;
let currentListing = null, galleryIndex = 0, lightboxIndex = 0;
let allItems = [];
let touchStartX = 0;
let heroImageLocked = false;

// ── Helpers ───────────────────────────────────────────────────────────────
const $      = id => document.getElementById(id);
const text   = v => (v == null) ? '' : String(v).trim();
const esc    = v => text(v).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const has    = v => !!text(v) && !['null','undefined','未知','none','[]'].includes(text(v).toLowerCase());
const fmt    = v => { const n=Number(v); return Number.isFinite(n)&&n>0 ? '$'+n.toLocaleString('en-US') : '' };
const first  = (o, keys) => keys.map(k=>o?.[k]).find(has)||'';
const fmtNum= n => Number.isFinite(n)&&n>0 ? n.toLocaleString('en-US') : '';
const fmtUpdated = v => {
  const m = text(v).match(/^(\d{4})-(\d{2})-(\d{2})/);
  return m ? `${Number(m[2])}月${Number(m[3])}日更新` : '';
};
const isInternalListing = i => /^QL-VERIFY-/i.test(text(i?.public_id));
function isApartment(facts) {
  return /公寓|condo|apartment/i.test([facts?.type, facts?.subtype].filter(Boolean).join(' '));
}
function layoutSummary(facts) {
  if (has(facts?.layout)) return text(facts.layout);
  return [
    facts?.bedrooms ? `${facts.bedrooms}房` : '',
    facts?.bathrooms ? `${facts.bathrooms}卫` : '',
  ].filter(Boolean).join('');
}

function listingLink(id) {
  const u = new URL(window.location.origin + window.location.pathname);
  if (has(id)) u.searchParams.set('id', text(id));
  return u.toString();
}
function advisorUrl(action, i) {
  const id = text(i?.public_id);
  const title = text(i?.title) || '金边出售房源';
  const price = fmt(i?.sale_price_usd);
  const lines = [
    '你好，想咨询这套金边出售房源',
    action ? `动作：${action}` : '',
    `房源：${title}`,
    price ? `总价：${price}` : '',
    id ? `系统编号：${id}` : '',
    '来源：侨联出售网站',
    id ? `链接：${listingLink(id)}` : '',
  ].filter(Boolean);
  return `${ADVISOR}?text=${encodeURIComponent(lines.join('\n'))}`;
}

// Parse title and use it only as a fallback when canonical fields are absent.
const num = v => {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : null;
};
function listValue(value) {
  if (Array.isArray(value)) return value.map(text).filter(has);
  if (!has(value)) return [];
  const raw = text(value);
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) return parsed.map(text).filter(has);
  } catch {}
  return raw.split(/[，,、|]/).map(v => v.trim()).filter(has);
}
function parseTitle(title) {
  const parts = text(title).split(/[｜|]/).map(s => s.trim()).filter(Boolean);
  const raw = {};
  for (const p of parts) {
    const beds = p.match(/(\d+)房/); if (beds) raw.bedrooms = +beds[1];
    const baths = p.match(/(\d+)卫/); if (baths) raw.bathrooms = +baths[1];
    const size = p.match(/(\d+(?:\.\d+)?)\s*㎡/); if (size) raw.size = +size[1];
  }
  return { raw, location: parts[0] || '' };
}
function listingFacts(i) {
  const parsed = parseTitle(text(i.title));
  return {
    project: text(i.project),
    location: text(i.location) || parsed.location,
    type: text(i.property_type),
    subtype: text(i.property_subtype),
    layout: text(i.layout),
    bedrooms: num(i.bedrooms) || parsed.raw.bedrooms || null,
    bathrooms: num(i.bathrooms) || parsed.raw.bathrooms || null,
    size: num(i.size_sqm) || parsed.raw.size || null,
    floor: text(i.floor),
  };
}
function statusInfo(value) {
  const v = text(value);
  if (v === '在售') return { label:v, className:'on-sale' };
  if (v === '已预留' || v === '待确认') return { label:v, className:'pending' };
  if (v === '已售' || v === '已下架') return { label:v, className:'sold' };
  return { label:v, className:'' };
}

function percent(v, digits = 1) {
  return Number.isFinite(v) ? `${v.toFixed(digits)}%` : '—';
}
function money(v) {
  return Number.isFinite(v) ? String.fromCharCode(36)+Math.round(v).toLocaleString('en-US') : '—';
}
function numberInput(id) {
  const n = Number($(id)?.value);
  return Number.isFinite(n) && n >= 0 ? n : null;
}
function renderSaleReference(i) {
  const block = $('saleReferenceBlock');
  if (!block) return;
  const ref = i?.sale_market_reference;
  if (!ref || Number(ref.count) < 2) {
    block.hidden = true;
    return;
  }
  block.hidden = false;
  $('saleReferenceScope').textContent = `${text(ref.label)} · 当前公开出售库存`;
  $('saleReferenceCount').textContent = `${fmtNum(Number(ref.count))} 套样本`;
  $('saleReferenceRange').textContent =
    `${fmt(ref.min_sale_price_usd)} – ${fmt(ref.max_sale_price_usd)}`;
  $('saleReferenceMedian').textContent = fmt(ref.median_sale_price_usd) || '—';
  const delta = Number(ref.current_vs_median_pct);
  $('saleReferencePosition').textContent = Number.isFinite(delta)
    ? (Math.abs(delta) < 0.05 ? '接近中位价' : delta > 0 ? `高 ${percent(Math.abs(delta))}` : `低 ${percent(Math.abs(delta))}`)
    : '—';

  const ppsmBlock = $('saleReferencePpsmBlock');
  const minPpsm = Number(ref.min_price_per_sqm_usd);
  const maxPpsm = Number(ref.max_price_per_sqm_usd);
  if (ppsmBlock && Number.isFinite(minPpsm) && minPpsm > 0 && Number.isFinite(maxPpsm) && maxPpsm > 0) {
    $('saleReferencePpsm').textContent = `${fmt(minPpsm)}/㎡ – ${fmt(maxPpsm)}/㎡`;
    ppsmBlock.hidden = false;
  } else if (ppsmBlock) {
    ppsmBlock.hidden = true;
  }
}
function calculateAcquisition() {
  if (!currentListing) return;
  const facts = listingFacts(currentListing);
  const ask = numberInput('calcAskPrice');
  const deal = numberInput('calcDealPrice');
  const stampRate = numberInput('calcStampRate') ?? 4;
  const other = numberInput('calcOtherCost') ?? 0;
  if (!deal) {
    ['calcDiscount','calcStampDuty','calcTotalAcquisition','calcAllInPpsm'].forEach(id => {
      if ($(id)) $(id).textContent = '—';
    });
    return;
  }
  const discount = ask && ask > 0 ? (deal - ask) / ask * 100 : NaN;
  const stamp = deal * stampRate / 100;
  const total = deal + stamp + other;
  $('calcDiscount').textContent = Number.isFinite(discount)
    ? (Math.abs(discount) < 0.05 ? '0%' : discount < 0 ? `低于挂牌 ${percent(Math.abs(discount))}` : `高于挂牌 ${percent(discount)}`)
    : '—';
  $('calcStampDuty').textContent = money(stamp);
  $('calcTotalAcquisition').textContent = money(total);
  $('calcAllInPpsm').textContent = facts.size && facts.size > 0 ? `${money(total / facts.size)}/㎡` : '—';
}
function resetInvestmentCalculator() {
  const price = Number(currentListing?.sale_price_usd);
  const value = Number.isFinite(price) && price > 0 ? String(price) : '';
  if ($('calcAskPrice')) $('calcAskPrice').value = value;
  if ($('calcDealPrice')) $('calcDealPrice').value = value;
  if ($('calcStampRate')) $('calcStampRate').value = '4';
  if ($('calcOtherCost')) $('calcOtherCost').value = '';
  calculateAcquisition();
}

// ── API ──────────────────────────────────────────────────────────────────
function queryParams() {
  const p = new URLSearchParams({ limit: String(LIMIT), offset: String(offset) });
  const q = text($('searchInput')?.value);
  const a = text($('area')?.value);
  const t = text($('type')?.value);
  const price = text($('price')?.value);
  const sort = text($('sortSelect')?.value) || 'newest';
  if (q) p.set('q', q);
  if (a) p.set('area', a);
  if (t) p.set('property_type', t);
  if (price) {
    const [mn, mx] = price.split('-');
    if (mn) p.set('min_price', mn);
    if (mx) p.set('max_price', mx);
  }
  p.set('sort', sort);
  return p;
}

async function loadMeta() {
  if (metadataLoaded) return;
  try {
    const r = await fetch(META_API, { cache:'no-store' });
    if (!r.ok) throw 0;
    const d = await r.json();
    const areas = Array.isArray(d.areas) ? d.areas : [];
    const types = Array.isArray(d.property_types) ? d.property_types : [];
    [...areas].filter(has).sort().forEach(v => {
      $('area')?.insertAdjacentHTML('beforeend',`<option value="${esc(v)}">${esc(v)}</option>`);
    });
    [...types].filter(v => has(v) && text(v) !== '未知').sort().forEach(v => {
      $('type')?.insertAdjacentHTML('beforeend',`<option value="${esc(v)}">${esc(v)}</option>`);
    });
    renderQuickFilters({ areas, types });
  } catch {}
  metadataLoaded = true;
}

async function loadList(reset = false) {
  if (reset) offset = 0;
  showSkeletons();
  setCount('读取中…');
  try {
    await loadMeta();
    if (!initialUrlRestored) {
      restoreFiltersFromUrl();
      initialUrlRestored = true;
    }
    const detailId = new URLSearchParams(window.location.search).get('id');
    const r = await fetch(API+'?'+queryParams(), { cache:'no-store' });
    if (!r.ok) throw 0;
    const d = await r.json();
    const rawItems = Array.isArray(d.items) ? d.items : [];
    const hiddenInternal = rawItems.filter(isInternalListing).length;
    allItems = rawItems.filter(i => !isInternalListing(i));
    const apiTotal = Number.isFinite(Number(d.total)) ? Number(d.total) : rawItems.length;
    total = Math.max(0, apiTotal - hiddenInternal);
    renderList();
    renderPager();
    updateHeroImage(allItems);
    updateUrlFilters(detailId);
    if (detailId && (!currentListing || text(currentListing.public_id) !== detailId)) {
      await openDetail(detailId, false);
    }
  } catch {
    allItems = [];
    setCount('');
    $('grid').innerHTML = emptyMarkup('暂时无法读取出售房源', [
      { label:'重新加载', action:'reload', primary:true },
      { label:'中文顾问', href:ADVISOR }
    ]);
  }
}

// Server-side sorting is used so pagination and ordering stay consistent.

// ── Render list ──────────────────────────────────────────────────────────
function renderList() {
  $('count').textContent = total ? `${fmtNum(total)} 套在售` : '暂无';
  $('heroCountNum').textContent = total > 0 ? fmtNum(total) : '—';
  $('grid').innerHTML = allItems.length
    ? allItems.map(item => cardMarkup(item)).join('')
    : emptyMarkup('当前条件下暂无出售房源', [
        { label:'清除筛选', action:'clear', primary:true },
        { label:'中文顾问', href:ADVISOR }
      ]);
  updateChips();
}
function setCount(txt) { if ($('count')) $('count').textContent = txt; }

// ── Card markup ──────────────────────────────────────────────────────────
function cardMarkup(i) {
  const id = text(i.public_id);
  const price = fmt(i.sale_price_usd);
  const title = text(i.title) || '金边出售房源';
  const facts = listingFacts(i);
  const displayTitle = facts.project || title;
  const status = statusInfo(i.status);
  const photoCount = Array.isArray(i.gallery_urls) ? i.gallery_urls.filter(has).length : 0;
  const imgUrl = text(i.cover_url);
  const layout = layoutSummary(facts);
  const specs = [
    layout,
    facts.size ? `${fmtNum(facts.size)}㎡` : '',
  ].filter(Boolean).join(' · ');
  const ppsm = isApartment(facts) && facts.size && num(i.sale_price_usd)
    ? `约 ${fmt(Math.round(Number(i.sale_price_usd) / facts.size))}/㎡` : '';
  const updated = fmtUpdated(i.updated_at);
  const descriptor = facts.project
    ? [facts.location, facts.subtype || facts.type].filter(Boolean)
        .filter((v, idx, arr) => arr.indexOf(v) === idx).join(' · ')
    : '';
  const telegramUrl = advisorUrl('询问实际价格', i);

  return `<article class="card">
    <button class="card-link" type="button" data-id="${esc(id)}" aria-label="查看 ${esc(title)}">
      <div class="card-image">
        ${imgUrl
          ? `<img src="${esc(imgUrl)}" alt="${esc(title)}" loading="lazy" onerror="this.style.display='none';this.nextElementSibling.style.display='grid'"><div class="image-fallback" style="display:none">暂无照片</div>`
          : `<div class="image-fallback">暂无照片</div>`}
        <div class="card-overlay"></div>
        ${status.label ? `<div class="card-status"><span class="status-tag ${esc(status.className)}">${esc(status.label)}</span></div>` : ''}
        ${photoCount > 1 ? `<div class="card-photo-count">${photoCount} 张</div>` : ''}
        ${price ? `<div class="card-price-overlay"><span class="card-price-text">${esc(price)}</span></div>` : ''}
      </div>
      <div class="card-body">
        <h2 class="card-title">${esc(displayTitle)}</h2>
        ${descriptor ? `<p class="card-type">${esc(descriptor)}</p>` : ''}
        ${specs ? `<p class="card-specs"><span>${esc(specs)}</span></p>` : ''}
        ${ppsm ? `<p class="card-specs"><span class="card-ppsm">${esc(ppsm)}</span></p>` : ''}
        ${updated ? `<p class="card-updated">${esc(updated)}</p>` : ''}
      </div>
    </button>
    <div class="card-actions">
      <button type="button" class="detail-btn" data-id="${esc(id)}">看详情</button>
      <a href="${telegramUrl}" target="_blank" rel="noopener">问价格</a>
    </div>
  </article>`;
}

// ── Detail ───────────────────────────────────────────────────────────────
async function openDetail(id, push = true) {
  try {
    if (isInternalListing({ public_id:id })) throw 0;
    const r = await fetch(API+'/'+encodeURIComponent(id), { cache:'no-store' });
    if (!r.ok) throw 0;
    const d = await r.json();
    const i = d.item || d.listing || d.data || d.result || d;
    if (!i || !has(i.public_id)) throw 0;

    currentListing = i;
    galleryIndex = 0;
    const facts = listingFacts(i);
    const layout = layoutSummary(facts);
    const price = fmt(i.sale_price_usd);
    const status = statusInfo(i.status);
    const desc = first(i,['description','description_text','adviser_copy','listing_description']);
    const amenities = listValue(first(i,['amenities','features','facilities']));

    $('detail-price').textContent = price || '价格咨询';
    $('detail-title').textContent = text(i.title) || '金边出售房源';
    $('detail-meta').textContent = [facts.location, layout, facts.subtype || facts.type]
      .filter(Boolean).filter((v, idx, arr) => arr.indexOf(v) === idx).join(' · ');

    const statusEl = $('detail-status');
    if (status.label) {
      statusEl.textContent = status.label;
      statusEl.className = `detail-status-tag ${status.className}`;
      statusEl.hidden = false;
    } else {
      statusEl.hidden = true;
    }

    if (isApartment(facts) && facts.size && num(i.sale_price_usd)) {
      $('detailPpsm').textContent = `${fmt(Math.round(Number(i.sale_price_usd) / facts.size))}/㎡`;
      $('detailPpsmBlock').hidden = false;
    } else {
      $('detailPpsmBlock').hidden = true;
    }

    const fields = [
      ['项目', facts.project],
      ['区域', facts.location],
      ['物业类型', facts.subtype || facts.type],
      ['户型', layout],
      ['面积', facts.size ? `${fmtNum(facts.size)} ㎡` : ''],
      ['楼层', facts.floor ? `${facts.floor} 楼` : ''],
      ['实拍', Array.isArray(i.gallery_urls) && i.gallery_urls.filter(has).length ? `${i.gallery_urls.filter(has).length} 张` : ''],
      ['状态', status.label],
      ['最近更新', fmtUpdated(i.updated_at)],
    ].filter(([,v])=>has(v));
    $('detail-fields').innerHTML = fields.map(([l,v]) =>
      `<div class="param"><small>${esc(l)}</small><strong>${esc(v)}</strong></div>`
    ).join('');

    if (has(desc)) {
      $('detail-description').textContent = desc;
      $('descriptionBlock').hidden = false;
    } else {
      $('descriptionBlock').hidden = true;
    }
    if (amenities.length) {
      $('detail-amenities').innerHTML = amenities.map(v=>`<span>${esc(v)}</span>`).join('');
      $('amenitiesBlock').hidden = false;
    } else {
      $('amenitiesBlock').hidden = true;
    }

    paintGallery();
    renderSaleReference(i);
    resetInvestmentCalculator();

    const unavailable = ['已售','已下架'].includes(status.label);
    const primary = $('detail-telegram');
    if (primary) {
      primary.textContent = '问实际价格';
      primary.href = advisorUrl('询问实际价格', i);
    }
    const booking = $('detail-book');
    if (booking) {
      booking.textContent = unavailable ? '找同类' : '预约看房';
      booking.href = advisorUrl(unavailable ? '寻找同区域同预算房源' : '预约现场/视频看房', i);
    }
    $('detail-footer-meta').textContent = [facts.location || text(i.title), price].filter(has).join(' · ').slice(0,46);

    $('overlay').hidden = false;
    document.body.style.overflow = 'hidden';
    if (push) {
      const u = new URL(window.location.href);
      u.searchParams.set('id', text(i.public_id));
      history.pushState({},'',u);
    }
  } catch {
    currentListing = null;
    $('overlay').hidden = true;
    document.body.style.overflow = '';
  }
}

function getGalleryImages() {
  if (!currentListing) return [];
  const imgs = [];
  if (has(currentListing.cover_url)) imgs.push(text(currentListing.cover_url));
  if (Array.isArray(currentListing.gallery_urls)) {
    currentListing.gallery_urls.filter(has).forEach(u => {
      const normalized = text(u);
      if (!imgs.includes(normalized)) imgs.push(normalized);
    });
  }
  return imgs;
}

function paintGallery() {
  const gallery = $('gallery');
  const imgs = getGalleryImages();
  gallery.innerHTML = imgs.length
    ? imgs.map((u,n) => `<img src="${esc(u)}" data-gallery-index="${n}" alt="房源照片 ${n+1}" loading="${n===0?'eager':'lazy'}">`).join('')
    : `<div class="image-fallback gallery-fallback">暂无房源照片</div>`;
  galleryIndex = Math.min(galleryIndex, Math.max(0, imgs.length - 1));
  gallery.style.transform = `translateX(-${galleryIndex*100}%)`;
  $('galleryCounter').textContent = imgs.length ? `${galleryIndex+1} / ${imgs.length}` : '0 / 0';
  $('galleryTotal').textContent = imgs.length;
  $('galleryAllBtn').hidden = imgs.length === 0;
  $('galleryPrev').hidden = imgs.length < 2;
  $('galleryNext').hidden = imgs.length < 2;
  paintDots(imgs.length);
}

function paintDots(count) {
  const dots = $('galleryDots');
  if (!dots) return;
  if (count > 12) { dots.innerHTML = ''; return; }
  dots.innerHTML = Array.from({length:count}, (_,n) =>
    `<button class="gallery-dot${n===galleryIndex?' active':''}" data-index="${n}" type="button" aria-label="查看第 ${n+1} 张"></button>`
  ).join('');
}

function stepGallery(step) {
  const imgs = getGalleryImages();
  if (imgs.length < 2) return;
  galleryIndex = (galleryIndex + step + imgs.length) % imgs.length;
  $('gallery').style.transform = `translateX(-${galleryIndex*100}%)`;
  $('galleryCounter').textContent = `${galleryIndex+1} / ${imgs.length}`;
  paintDots(imgs.length);
}

function openLightbox(index = galleryIndex) {
  const imgs = getGalleryImages();
  if (!imgs.length) return;
  lightboxIndex = Math.max(0, Math.min(Number(index)||0, imgs.length-1));
  renderLightbox();
  $('lightbox').hidden = false;
}
function renderLightbox() {
  const imgs = getGalleryImages();
  if (!imgs.length) return;
  lightboxIndex = (lightboxIndex + imgs.length) % imgs.length;
  $('lightboxImage').src = imgs[lightboxIndex];
  $('lightboxImage').alt = `房源大图 ${lightboxIndex+1}`;
  $('lightboxCount').textContent = `${lightboxIndex+1} / ${imgs.length}`;
  $('lightboxPrev').hidden = imgs.length < 2;
  $('lightboxNext').hidden = imgs.length < 2;
}
function stepLightbox(step) {
  const imgs = getGalleryImages();
  if (imgs.length < 2) return;
  lightboxIndex = (lightboxIndex + step + imgs.length) % imgs.length;
  renderLightbox();
}
function closeLightbox() {
  $('lightbox').hidden = true;
  $('lightboxImage').removeAttribute('src');
}

function closeDetail(updateUrl = true) {
  closeLightbox();
  $('overlay').hidden = true;
  document.body.style.overflow = '';
  currentListing = null;
  if (updateUrl) {
    const u = new URL(window.location.href);
    u.searchParams.delete('id');
    history.pushState({},'',u);
  }
}

// ── Filters & chips ──────────────────────────────────────────────────────
function clearFilters() {
  if ($('searchInput')) $('searchInput').value = '';
  if ($('area')) $('area').value = '';
  if ($('type')) $('type').value = '';
  if ($('price')) $('price').value = '';
  if ($('sortSelect')) $('sortSelect').value = 'newest';
  loadList(true);
}

function updateChips() {
  const chips = [];
  const q = text($('searchInput')?.value);
  const area = text($('area')?.value);
  const type = text($('type')?.value);
  const price = text($('price')?.value);
  const priceLabels = {
    '0-80000':'$8万内','80000-120000':'$8–12万','120000-200000':'$12–20万',
    '200000-350000':'$20–35万','350000-999999999':'$35万+'
  };
  if (q) chips.push({key:'q',label:q});
  if (area) chips.push({key:'area',label:area});
  if (type) chips.push({key:'type',label:type});
  if (price) chips.push({key:'price',label:priceLabels[price]||price});

  const row = $('chipsRow');
  const container = $('activeChips');
  if (!row || !container) return;
  if (!chips.length) {
    row.hidden = true;
    container.innerHTML = '';
    updateQuickFilterState();
    return;
  }
  row.hidden = false;
  container.innerHTML = chips.map(c =>
    `<button class="chip" type="button" data-clear-filter="${esc(c.key)}">${esc(c.label)}<span class="chip-remove" aria-hidden="true">×</span></button>`
  ).join('');
  updateQuickFilterState();
}

function updateUrlFilters(detailId = null) {
  const params = new URLSearchParams();
  const q = text($('searchInput')?.value);
  const area = text($('area')?.value);
  const type = text($('type')?.value);
  const price = text($('price')?.value);
  const sort = text($('sortSelect')?.value);
  if (q) params.set('q',q);
  if (area) params.set('area',area);
  if (type) params.set('type',type);
  if (price) params.set('price',price);
  if (sort && sort !== 'newest') params.set('sort',sort);
  if (detailId) params.set('id',detailId);
  const search = params.toString();
  history.replaceState({},'',search ? `?${search}` : window.location.pathname);
}

function restoreFiltersFromUrl() {
  const p = new URLSearchParams(window.location.search);
  const q = p.get('q'); if (q && $('searchInput')) $('searchInput').value = q;
  const area = p.get('area'); if (area && $('area')) $('area').value = area;
  const type = p.get('type'); if (type && $('type')) $('type').value = type;
  const price = p.get('price'); if (price && $('price')) $('price').value = price;
  const sort = p.get('sort'); if (sort && $('sortSelect')) $('sortSelect').value = sort === 'updated_desc' ? 'newest' : sort;
}

function renderQuickFilters(meta = {}) {
  const areas = Array.isArray(meta.areas) ? meta.areas.filter(has) : [];
  const types = Array.isArray(meta.types) ? meta.types.filter(has) : [];
  const hasArea = needle => areas.some(v => text(v).includes(needle));
  const hasType = needle => types.some(v => text(v) === needle);
  const presets = [
    { kind:'price', value:'0-80000', label:'$8万内', show:true },
    { kind:'price', value:'80000-120000', label:'$8–12万', show:true },
    { kind:'area', value:'BKK1', label:'BKK1', show:hasArea('BKK1') },
    { kind:'area', value:'永旺3附近', label:'永旺3附近', show:hasArea('永旺3附近') },
    { kind:'type', value:'公寓', label:'公寓', show:hasType('公寓') },
    { kind:'type', value:'别墅', label:'别墅', show:hasType('别墅') },
  ].filter(x => x.show);
  const root = $('quickFilters');
  if (!root) return;
  root.innerHTML = presets.map(p =>
    `<button class="quick-filter-chip" type="button" data-quick-kind="${esc(p.kind)}" data-quick-value="${esc(p.value)}">${esc(p.label)}</button>`
  ).join('');
  updateQuickFilterState();
}

function updateQuickFilterState() {
  const root = $('quickFilters');
  if (!root) return;
  const current = {
    area: text($('area')?.value),
    type: text($('type')?.value),
    price: text($('price')?.value),
  };
  root.querySelectorAll('[data-quick-kind]').forEach(button => {
    const kind = button.dataset.quickKind;
    button.classList.toggle('active', current[kind] === button.dataset.quickValue);
  });
}

// ── Skeleton / empty ─────────────────────────────────────────────────────
function showSkeletons() {
  $('grid').innerHTML = Array(6).fill('<div class="skeleton" aria-hidden="true"></div>').join('');
}
function emptyMarkup(msg, actions = []) {
  const btns = actions.map(a =>
    a.href
      ? `<a href="${esc(a.href)}" ${a.primary?'class="primary"':''} target="_blank" rel="noopener">${esc(a.label)}</a>`
      : `<button type="button" ${a.primary?'class="primary"':''} data-empty-action="${esc(a.action)}">${esc(a.label)}</button>`
  ).join('');
  return `<div class="empty"><p>${esc(msg)}</p>${btns ? `<div class="empty-actions">${btns}</div>` : ''}</div>`;
}

// ── Pager ────────────────────────────────────────────────────────────────
function renderPager() {
  const pages = Math.max(1, Math.ceil(total / LIMIT));
  const pager = $('pager');
  if (pager) pager.hidden = total <= LIMIT;
  if ($('pageno')) $('pageno').textContent = `${Math.floor(offset/LIMIT)+1} / ${pages}`;
  if ($('prev')) $('prev').disabled = offset === 0;
  if ($('next')) $('next').disabled = offset + allItems.length >= total;
}

function updateHeroImage(items) {
  if (heroImageLocked) return;
  const hero = items.find(i => has(i.cover_url));
  const img = $('heroImage');
  if (!img || !hero) return;
  img.hidden = false;
  img.src = text(hero.cover_url);
  img.alt = '';
  img.onload = () => { heroImageLocked = true; };
  img.onerror = () => { img.hidden = true; };
}

// ── Events ──────────────────────────────────────────────────────────────
$('searchBtn')?.addEventListener('click', () => loadList(true));
$('searchInput')?.addEventListener('keydown', e => {
  if (e.key === 'Enter') { e.preventDefault(); loadList(true); }
});
$('resetBtn')?.addEventListener('click', clearFilters);
['area','type','price'].forEach(id => {
  $(id)?.addEventListener('change', () => loadList(true));
});
$('sortSelect')?.addEventListener('change', () => loadList(true));

$('quickFilters')?.addEventListener('click', e => {
  const button = e.target.closest('[data-quick-kind]');
  if (!button) return;
  const kind = button.dataset.quickKind;
  const value = button.dataset.quickValue;
  const target = kind === 'area' ? $('area') : kind === 'type' ? $('type') : kind === 'price' ? $('price') : null;
  if (!target) return;
  target.value = target.value === value ? '' : value;
  loadList(true);
});

$('activeChips')?.addEventListener('click', e => {
  const button = e.target.closest('[data-clear-filter]');
  if (!button) return;
  const key = button.dataset.clearFilter;
  if (key === 'q' && $('searchInput')) $('searchInput').value = '';
  if (key === 'area' && $('area')) $('area').value = '';
  if (key === 'type' && $('type')) $('type').value = '';
  if (key === 'price' && $('price')) $('price').value = '';
  loadList(true);
});

['calcDealPrice','calcStampRate','calcOtherCost'].forEach(id => {
  $(id)?.addEventListener('input', calculateAcquisition);
});
$('investmentCalcReset')?.addEventListener('click', resetInvestmentCalculator);

$('grid')?.addEventListener('click', e => {
  const emptyAction = e.target.closest('[data-empty-action]');
  if (emptyAction) {
    if (emptyAction.dataset.emptyAction === 'clear') clearFilters();
    else loadList(true);
    return;
  }
  if (e.target.closest('a')) return;
  const button = e.target.closest('[data-id]');
  if (button) openDetail(button.dataset.id);
});

$('closeBtn')?.addEventListener('click', () => closeDetail());
$('overlay')?.addEventListener('click', e => { if (e.target === $('overlay')) closeDetail(); });
$('galleryPrev')?.addEventListener('click', () => stepGallery(-1));
$('galleryNext')?.addEventListener('click', () => stepGallery(1));
$('galleryAllBtn')?.addEventListener('click', () => openLightbox(galleryIndex));
$('gallery')?.addEventListener('click', e => {
  const image = e.target.closest('img[data-gallery-index]');
  if (image) openLightbox(Number(image.dataset.galleryIndex));
});
$('galleryDots')?.addEventListener('click', e => {
  const dot = e.target.closest('[data-index]');
  if (!dot) return;
  galleryIndex = Number(dot.dataset.index) || 0;
  stepGallery(0);
});

$('lightboxClose')?.addEventListener('click', closeLightbox);
$('lightboxPrev')?.addEventListener('click', () => stepLightbox(-1));
$('lightboxNext')?.addEventListener('click', () => stepLightbox(1));
$('lightbox')?.addEventListener('click', e => { if (e.target === $('lightbox')) closeLightbox(); });

$('prev')?.addEventListener('click', () => {
  offset = Math.max(0, offset-LIMIT);
  loadList(false);
  window.scrollTo({top:$('catalog')?.offsetTop || 0, behavior:'smooth'});
});
$('next')?.addEventListener('click', () => {
  offset += LIMIT;
  loadList(false);
  window.scrollTo({top:$('catalog')?.offsetTop || 0, behavior:'smooth'});
});

window.addEventListener('popstate', () => {
  const id = new URLSearchParams(window.location.search).get('id');
  if (id) openDetail(id, false);
  else if (!$('overlay').hidden) closeDetail(false);
});
window.addEventListener('keydown', e => {
  if (!$('lightbox').hidden) {
    if (e.key === 'Escape') closeLightbox();
    if (e.key === 'ArrowLeft') stepLightbox(-1);
    if (e.key === 'ArrowRight') stepLightbox(1);
    return;
  }
  if (!$('overlay').hidden) {
    if (e.key === 'Escape') closeDetail();
    if (e.key === 'ArrowLeft') stepGallery(-1);
    if (e.key === 'ArrowRight') stepGallery(1);
  }
});
document.addEventListener('touchstart', e => { touchStartX = e.changedTouches[0].screenX; }, {passive:true});
document.addEventListener('touchend', e => {
  const dx = e.changedTouches[0].screenX - touchStartX;
  if (Math.abs(dx) <= 50) return;
  if (!$('lightbox').hidden) stepLightbox(dx < 0 ? 1 : -1);
  else if (!$('overlay').hidden) stepGallery(dx < 0 ? 1 : -1);
}, {passive:true});

// ── Boot ────────────────────────────────────────────────────────────────
restoreFiltersFromUrl();
loadList();
