const API = '/api/v3/sale/listings'
const META_API = '/api/v3/sale/meta'
const ADVISOR = 'https://t.me/pengqingw'
const LIMIT = 24
let offset = 0, total = 0, metadataLoaded = false, currentListing = null, galleryIndex = 0
const $ = (id) => document.getElementById(id)
const text = (v) => v == null ? '' : String(v).trim()
const escapeHtml = (v) => text(v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))
const hasValue = (v) => !!text(v) && !['null','undefined','未知','none','[]'].includes(text(v).toLowerCase())
const formatPrice = (v) => { const n=Number(v); return Number.isFinite(n)&&n>0?'$'+n.toLocaleString('en-US'):'' }
const first = (o, keys) => keys.map(k=>o?.[k]).find(hasValue) || ''
const listValue = (v) => Array.isArray(v) ? v.filter(hasValue) : (hasValue(v) ? text(v).split(/[,，、|]/).map(s=>s.trim()).filter(Boolean) : [])
const projectOf = i => first(i,['project','building','project_name'])
const locationOf = i => first(i,['location','area','district'])
const bedroomsOf = i => { const n=Number(first(i,['bedrooms','bedroom_count'])); return Number.isFinite(n)&&n>0?n:0 }
const sizeOf = i => { const n=Number(first(i,['size_sqm','area_sqm','size'])); return Number.isFinite(n)&&n>0?n:0 }
const priceOf = i => first(i,['sale_price_usd','sale_price','price_usd','price'])
const descriptionOf = i => first(i,['description','description_text','listing_description','adviser_copy'])
const amenitiesOf = i => listValue(first(i,['amenities','features','facilities']))
const normalizeListing = (payload) => payload?.item || payload?.listing || payload?.data || payload?.result || payload
const mediaUrl = (value) => text(value)
const advisorUrl = i => ADVISOR+'?text='+encodeURIComponent(`你好，想咨询金边出售房源 ${text(i.public_id)}`)
const retryImage = (image) => {
  if (image.dataset.retried) {
    image.hidden = true
    image.nextElementSibling.hidden = false
    return
  }
  image.dataset.retried = '1'
  image.src = image.src
}
const retryGalleryImage = (image) => {
  if (image.dataset.retried) {
    image.replaceWith(Object.assign(document.createElement('div'), { className:'image-fallback', textContent:'图片暂时无法显示' }))
    return
  }
  image.dataset.retried = '1'
  image.src = image.src
}

function queryParams(){const p=new URLSearchParams({limit:LIMIT,offset}); const area=$('area').value,type=$('type').value,price=$('price').value; if(area)p.set('area',area); if(type)p.set('property_type',type); if(price){const[min,max]=price.split('-');p.set('min_price',min);p.set('max_price',max)} return p}
async function loadMetadata(){if(metadataLoaded)return; const r=await fetch(META_API,{cache:'no-store'}); if(!r.ok)throw Error('metadata'); const d=await r.json(),areas=d.areas||[],types=d.property_types||[],minPrice=Number(d.min_price_usd),maxPrice=Number(d.max_price_usd); [...areas].filter(hasValue).sort().forEach(v=>$('area').insertAdjacentHTML('beforeend',`<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`)); [...types].filter(hasValue).sort().forEach(v=>$('type').insertAdjacentHTML('beforeend',`<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`)); const prices=[minPrice,maxPrice].filter(Number.isFinite); if(prices.length===2){$('price').setAttribute('aria-description',`真实出售价格范围 ${formatPrice(Math.min(...prices))} 至 ${formatPrice(Math.max(...prices))}`)} metadataLoaded=true}
function showSkeletons(){$('grid').innerHTML='<div class="skeleton"></div><div class="skeleton"></div><div class="skeleton"></div>'}
function summary(i){const b=bedroomsOf(i),s=sizeOf(i); return [locationOf(i),b?`${b}室`:'',hasValue(i.layout)?i.layout:'',s?`${s.toLocaleString()}㎡`:''].filter(hasValue).join(' · ')}
function imageMarkup(i,index=0){return hasValue(i.cover_url)?`<img src="${escapeHtml(mediaUrl(i.cover_url))}" alt="${escapeHtml(text(i.title)||projectOf(i)||'出售房源')}" loading="${index===0?'eager':'lazy'}" fetchpriority="${index===0?'high':'auto'}" onerror="retryImage(this)"><div class="image-fallback" hidden>暂无房源照片</div>`:'<div class="image-fallback">暂无房源照片</div>'}
function card(i){const id=escapeHtml(i.public_id),title=text(i.title)||projectOf(i)||id,price=formatPrice(priceOf(i));return `<article class="card"><button class="card-open" type="button" data-id="${id}" style="display:block;width:100%;padding:0;border:0;background:none;text-align:left"><div class="card-image">${imageMarkup(i)}<div class="card-overlay"></div><div class="card-meta"><span class="tag">${id}</span>${hasValue(i.status)?`<span class="tag">${escapeHtml(i.status)}</span>`:''}</div>${price?`<div class="card-price"><small>出售总价</small><strong class="price">${price}</strong></div>`:''}</div><div class="card-body"><h2 class="card-title">${escapeHtml(title)}</h2>${summary(i)?`<p class="card-summary">${escapeHtml(summary(i))}</p>`:''}</div></button><div class="card-actions"><button type="button" data-id="${id}" class="detail-action">查看详情</button><a href="${advisorUrl(i)}" target="_blank" rel="noopener">咨询这套</a></div></article>`}
function render(items){$('count').textContent=total?`${total} 套在售`:'暂无';$('grid').innerHTML=items.length?items.map(card).join(''):'<div class="empty">当前条件下暂无出售房源</div>'}
function updatePager(){const pages=Math.max(1,Math.ceil(total/LIMIT));$('pager').hidden=total<=LIMIT;$('pageno').textContent=`${Math.floor(offset/LIMIT)+1} / ${pages}`;$('prev').disabled=offset===0;$('next').disabled=offset+LIMIT>=total}
async function load(reset=false){if(reset)offset=0;showSkeletons();$('count').textContent='读取中';try{await loadMetadata();const r=await fetch(API+'?'+queryParams(),{cache:'no-store'});if(!r.ok)throw Error('list');const d=await r.json();const items=d.items||[];if(!$('bedrooms').options.length||$('bedrooms').options.length===1){[...new Set(items.map(bedroomsOf).filter(Boolean))].sort((a,b)=>a-b).forEach(v=>$('bedrooms').insertAdjacentHTML('beforeend',`<option value="${v}">${v}室</option>`))}const selectedBedrooms=Number($('bedrooms').value);const visibleItems=selectedBedrooms?items.filter(i=>bedroomsOf(i)===selectedBedrooms):items;total=selectedBedrooms?visibleItems.length:Number(d.total)||0;render(visibleItems);updatePager();const id=new URLSearchParams(location.search).get('id');if(id&&!currentListing)openDetail(id,false)}catch{$('count').textContent='';$('grid').innerHTML='<div class="empty error">出售房源暂时无法读取，请稍后刷新。</div>'}}

async function openDetail(id,push=true){try{const r=await fetch(API+'/'+encodeURIComponent(id),{cache:'no-store'});if(!r.ok)throw Error('detail');const d=await r.json();currentListing=normalizeListing(d);if(!currentListing||!hasValue(currentListing.public_id))throw Error('detail-schema');galleryIndex=0;const i=currentListing;$('detail-id').textContent=text(i.public_id);$('detail-price').textContent=formatPrice(priceOf(i));$('detail-title').textContent=text(i.title)||projectOf(i)||text(i.public_id);$('consultButton').href=advisorUrl(i);$('consultContext').textContent=[text(i.public_id),projectOf(i),formatPrice(priceOf(i))].filter(hasValue).join(' · ');const type=[i.property_type,i.property_subtype].filter(hasValue).join(' / '),b=bedroomsOf(i),s=sizeOf(i);const fields=[['项目',projectOf(i)],['位置',locationOf(i)],['物业类型',type],['户型',i.layout],['卧室',b?b+' 间':''],['卫浴',Number(i.bathrooms)>0?i.bathrooms+' 间':''],['面积',s?s.toLocaleString()+'㎡':''],['楼层',hasValue(i.floor)?i.floor+' 楼':'']].filter(([,v])=>hasValue(v));$('detail-fields').innerHTML=fields.map(([l,v])=>`<div class="field"><small>${escapeHtml(l)}</small><strong>${escapeHtml(v)}</strong></div>`).join('');const desc=descriptionOf(i);$('descriptionBlock').hidden=!hasValue(desc);$('detail-description').textContent=desc;const amenities=amenitiesOf(i);$('amenitiesBlock').hidden=!amenities.length;$('detail-amenities').innerHTML=amenities.map(v=>`<span>${escapeHtml(v)}</span>`).join('');paintGallery();$('overlay').hidden=false;document.body.style.overflow='hidden';if(push){const u=new URL(location.href);u.searchParams.set('id',id);history.pushState({},'',u)}}catch{$('overlay').hidden=true;document.body.style.overflow='';currentListing=null}}
function galleryImages(){const imgs=[...(currentListing?.gallery_urls||[])].filter(hasValue).map(mediaUrl);const cover=mediaUrl(currentListing?.cover_url);if(hasValue(cover)&&!imgs.includes(cover))imgs.unshift(cover);return imgs}
function paintGallery(){const imgs=galleryImages();$('gallery').innerHTML=imgs.length?imgs.map((u,n)=>`<img src="${escapeHtml(u)}" alt="房源照片 ${n+1}" loading="${n===galleryIndex?'eager':'lazy'}" fetchpriority="${n===galleryIndex?'high':'auto'}" onerror="retryGalleryImage(this)">`).join(''):'<div class="image-fallback" style="width:100%;flex:0 0 100%">暂无房源照片</div>';$('gallery').style.transform=`translateX(-${galleryIndex*100}%)`;$('galleryCount').textContent=imgs.length?`${galleryIndex+1} / ${imgs.length}`:'0 / 0';$('galleryPrev').hidden=imgs.length<2;$('galleryNext').hidden=imgs.length<2}
function stepGallery(step){const imgs=galleryImages();if(imgs.length>1){galleryIndex=(galleryIndex+step+imgs.length)%imgs.length;paintGallery()}}
function closeDetail(updateUrl=true){$('overlay').hidden=true;document.body.style.overflow='';currentListing=null;if(updateUrl){const u=new URL(location.href);u.searchParams.delete('id');history.pushState({},'',u)}}
function resetFilters(){$('area').value='';$('type').value='';$('bedrooms').value='';$('price').value=''}
$('brandButton').addEventListener('click',()=>{closeDetail();resetFilters();load(true)});$('resetButton').addEventListener('click',()=>{resetFilters();load(true)});['area','type','bedrooms','price'].forEach(id=>$(id).addEventListener('change',()=>load(true)));$('grid').addEventListener('click',e=>{const t=e.target.closest('[data-id]');if(t&&!e.target.closest('a'))openDetail(t.dataset.id)});$('closeButton').addEventListener('click',()=>closeDetail());$('overlay').addEventListener('click',e=>{if(e.target===$('overlay'))closeDetail()});$('galleryPrev').addEventListener('click',()=>stepGallery(-1));$('galleryNext').addEventListener('click',()=>stepGallery(1));$('prev').addEventListener('click',()=>{offset-=LIMIT;load()});$('next').addEventListener('click',()=>{offset+=LIMIT;load()});window.addEventListener('popstate',()=>{const id=new URLSearchParams(location.search).get('id');id?openDetail(id,false):closeDetail(false)});window.addEventListener('keydown',e=>{if(e.key==='Escape'&&!$('overlay').hidden)closeDetail()});load()
