const API = '/api/v3/sale/listings'
const ADVISOR = 'https://t.me/pengqingw'
const LIMIT = 24
let offset = 0
let total = 0
let metadataLoaded = false
let currentListing = null
let galleryIndex = 0

const $ = (id) => document.getElementById(id)
const text = (value) => value == null ? '' : String(value).trim()
const escapeHtml = (value) => text(value).replace(/[&<>"']/g, (char) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]))
const formatPrice = (value) => { const number = Number(value); return Number.isFinite(number) && number > 0 ? '$' + number.toLocaleString('en-US') : '' }
const hasValue = (value) => text(value) && text(value).toLowerCase() !== 'null' && text(value).toLowerCase() !== 'undefined' && text(value) !== '未知'

function queryParams() {
  const params = new URLSearchParams({ limit: LIMIT, offset })
  const area = $('area').value
  const type = $('type').value
  const price = $('price').value
  if (area) params.set('area', area)
  if (type) params.set('property_type', type)
  if (price) { const [min, max] = price.split('-'); params.set('min_price', min); params.set('max_price', max) }
  return params
}

async function loadMetadata() {
  if (metadataLoaded) return
  const response = await fetch(API + '?limit=100&offset=0', { cache: 'no-store' })
  if (!response.ok) throw new Error('metadata')
  const data = await response.json()
  const areas = new Set(), types = new Set()
  ;(data.items || []).forEach((item) => {
    ;[item.project, item.location].filter(hasValue).forEach((value) => areas.add(text(value)))
    if (hasValue(item.property_type)) types.add(text(item.property_type))
  })
  ;[...areas].sort().forEach((value) => $('area').insertAdjacentHTML('beforeend', `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`))
  ;[...types].sort().forEach((value) => $('type').insertAdjacentHTML('beforeend', `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`))
  metadataLoaded = true
}

function showSkeletons() { $('grid').innerHTML = '<div class="skeleton"></div><div class="skeleton"></div><div class="skeleton"></div>' }
function summary(item) { return [item.project, item.location, item.layout, item.property_type, Number(item.size_sqm) > 0 ? Number(item.size_sqm).toLocaleString() + '㎡' : ''].filter(hasValue).join(' · ') }
function imageMarkup(item) { return hasValue(item.cover_url) ? `<img src="${escapeHtml(item.cover_url)}" alt="${escapeHtml(text(item.title) || text(item.project) || '出售房源')}" loading="lazy" onerror="this.hidden=true">` : '<div class="image-fallback">暂无房源照片</div>' }
function card(item) {
  const id = escapeHtml(item.public_id)
  const title = text(item.title) || text(item.project)
  const price = formatPrice(item.sale_price_usd)
  return `<article class="card"><button class="card-open" type="button" data-id="${id}" style="display:block;width:100%;padding:0;border:0;background:none;text-align:left"><div class="card-image">${imageMarkup(item)}<div class="card-overlay"></div><div class="card-meta"><span class="tag">${id}</span>${hasValue(item.status) ? `<span class="tag">${escapeHtml(item.status)}</span>` : ''}</div>${price ? `<div class="card-price"><small>出售总价</small><strong class="price">${price}</strong></div>` : ''}</div><div class="card-body"><h2 class="card-title">${escapeHtml(title)}</h2>${summary(item) ? `<p class="card-summary">${escapeHtml(summary(item))}</p>` : ''}</div></button><div class="card-actions"><button type="button" data-id="${id}" class="detail-action">查看详情</button><a href="${ADVISOR}?text=${encodeURIComponent('你好，想咨询金边出售房源 ' + text(item.public_id))}" target="_blank" rel="noopener">咨询这套</a></div></article>`
}
function render(items) { $('count').textContent = total ? `${total} 套在售` : '暂无'; $('grid').innerHTML = items.length ? items.map(card).join('') : '<div class="empty">当前条件下暂无出售房源</div>' }
function updatePager() { const pages = Math.max(1, Math.ceil(total / LIMIT)); $('pager').hidden = total <= LIMIT; $('pageno').textContent = `${Math.floor(offset / LIMIT) + 1} / ${pages}`; $('prev').disabled = offset === 0; $('next').disabled = offset + LIMIT >= total }
async function load(reset = false) { if (reset) offset = 0; showSkeletons(); $('count').textContent = '读取中'; try { await loadMetadata(); const response = await fetch(API + '?' + queryParams(), { cache: 'no-store' }); if (!response.ok) throw new Error('list'); const data = await response.json(); total = Number(data.total) || 0; render(data.items || []); updatePager(); const deepLink = new URLSearchParams(location.search).get('id'); if (deepLink && !currentListing) openDetail(deepLink, false) } catch { $('count').textContent = ''; $('grid').innerHTML = '<div class="empty error">出售房源暂时无法读取，请稍后刷新。</div>' } }

async function openDetail(id, push = true) { try { const response = await fetch(API + '/' + encodeURIComponent(id), { cache: 'no-store' }); if (!response.ok) throw new Error('detail'); const data = await response.json(); currentListing = data.item || data; galleryIndex = 0; const item = currentListing; $('detail-id').textContent = text(item.public_id); $('detail-price').textContent = formatPrice(item.sale_price_usd); $('detail-title').textContent = text(item.title) || text(item.project); $('consultButton').href = ADVISOR + '?text=' + encodeURIComponent('你好，想咨询金边出售房源 ' + text(item.public_id)); const type = [item.property_type, item.property_subtype].filter(hasValue).join(' / '); const fields = [['项目', item.project], ['位置', item.location], ['物业类型', type], ['户型', item.layout], ['卧室', hasValue(item.bedrooms) && Number(item.bedrooms) > 0 ? item.bedrooms + ' 间' : ''], ['卫浴', hasValue(item.bathrooms) && Number(item.bathrooms) > 0 ? item.bathrooms + ' 间' : ''], ['面积', Number(item.size_sqm) > 0 ? Number(item.size_sqm).toLocaleString() + '㎡' : ''], ['楼层', hasValue(item.floor) ? item.floor + ' 楼' : '']].filter(([, value]) => hasValue(value)); $('detail-fields').innerHTML = fields.map(([label, value]) => `<div class="field"><small>${escapeHtml(label)}</small><strong>${escapeHtml(value)}</strong></div>`).join(''); paintGallery(); $('overlay').hidden = false; document.body.style.overflow = 'hidden'; if (push) { const url = new URL(location.href); url.searchParams.set('id', id); history.pushState({}, '', url) } } catch { $('overlay').hidden = true } }
function galleryImages() { const images = [...(currentListing?.gallery_urls || [])].filter(hasValue); if (hasValue(currentListing?.cover_url) && !images.includes(currentListing.cover_url)) images.unshift(currentListing.cover_url); return images }
function paintGallery() { const images = galleryImages(); $('gallery').innerHTML = images.length ? images.map((url) => `<img src="${escapeHtml(url)}" alt="房源照片 ${galleryIndex + 1}" loading="lazy">`).join('') : '<div class="image-fallback" style="width:100%;flex:0 0 100%">暂无房源照片</div>'; $('gallery').style.transform = `translateX(-${galleryIndex * 100}%)`; $('galleryCount').textContent = images.length ? `${galleryIndex + 1} / ${images.length}` : '0 / 0'; $('galleryPrev').hidden = images.length < 2; $('galleryNext').hidden = images.length < 2 }
function stepGallery(step) { const images = galleryImages(); if (images.length > 1) { galleryIndex = (galleryIndex + step + images.length) % images.length; paintGallery() } }
function closeDetail(updateUrl = true) { $('overlay').hidden = true; document.body.style.overflow = ''; currentListing = null; if (updateUrl) { const url = new URL(location.href); url.searchParams.delete('id'); history.pushState({}, '', url) } }

$('brandButton').addEventListener('click', () => { closeDetail(); $('area').value = ''; $('type').value = ''; $('price').value = ''; load(true) })
$('resetButton').addEventListener('click', () => { $('area').value = ''; $('type').value = ''; $('price').value = ''; load(true) })
;['area', 'type', 'price'].forEach((id) => $(id).addEventListener('change', () => load(true)))
$('grid').addEventListener('click', (event) => { const target = event.target.closest('[data-id]'); if (target && !event.target.closest('a')) openDetail(target.dataset.id) })
$('closeButton').addEventListener('click', () => closeDetail())
$('overlay').addEventListener('click', (event) => { if (event.target === $('overlay')) closeDetail() })
$('galleryPrev').addEventListener('click', () => stepGallery(-1)); $('galleryNext').addEventListener('click', () => stepGallery(1)); $('prev').addEventListener('click', () => { offset -= LIMIT; load() }); $('next').addEventListener('click', () => { offset += LIMIT; load() })
window.addEventListener('popstate', () => { const id = new URLSearchParams(location.search).get('id'); id ? openDetail(id, false) : closeDetail(false) })
window.addEventListener('keydown', (event) => { if (event.key === 'Escape' && !$('overlay').hidden) closeDetail() })
load()
