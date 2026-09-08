from __future__ import annotations

import json
import sqlite3
from datetime import date
from pathlib import Path

import pytest

from qiaolian_v3.db.migration_runner import MigrationRunner
from qiaolian_v3.legacy.callback_compat import resolve_legacy_callback
from qiaolian_v3.user_bot import HOME_ROWS, REACHABLE_SERVICES, start_screen
from qiaolian_v3.user_bot.appointments import AppointmentFlow
from qiaolian_v3.user_bot.deeplinks import build_property_payload, parse_start_payload, require_ql_identity
from qiaolian_v3.user_bot.listing_actions import action_policy
from qiaolian_v3.user_bot.photos import PhotoService
from qiaolian_v3.user_bot.repository import AppointmentRepository, UserListingRepository
from qiaolian_v3.user_bot.service import UserBotService


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    MigrationRunner(conn).migrate()
    conn.executescript('''
    CREATE TABLE appointments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,username TEXT NOT NULL DEFAULT '',
        display_name TEXT NOT NULL DEFAULT '',listing_id TEXT NOT NULL DEFAULT '',viewing_mode TEXT NOT NULL DEFAULT '',
        appointment_date TEXT NOT NULL DEFAULT '',appointment_time TEXT NOT NULL DEFAULT '',contact_value TEXT NOT NULL DEFAULT '',
        note TEXT NOT NULL DEFAULT '',status TEXT NOT NULL DEFAULT 'pending',created_at TEXT NOT NULL
    );
    CREATE TABLE tenant_bindings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,binding_code TEXT NOT NULL UNIQUE,
        property_name TEXT NOT NULL DEFAULT '',lease_end_date TEXT NOT NULL DEFAULT '',rent_day INTEGER,
        monthly_rent REAL NOT NULL DEFAULT 0
    );
    ''')
    return conn


def seed_listing(conn: sqlite3.Connection, *, listing_id: int, ql: str, status: str = 'active', project: str = '富力城', location: str = 'BKK1', gallery=None):
    source_id = listing_id
    post_id = listing_id
    revision_id = listing_id
    canonical_id = listing_id
    offer_id = listing_id
    conn.execute("INSERT INTO v3_sources(id,source_type,source_name,external_identity) VALUES (?,?,?,?)", (source_id,'telegram',f's{listing_id}',f's{listing_id}'))
    conn.execute("INSERT INTO v3_source_posts(id,source_id,source_identity_key,source_mode,source_type,source_name,external_post_id) VALUES (?,?,?,?,?,?,?)", (post_id,source_id,f'sp{listing_id}','collector','telegram',f's{listing_id}',str(listing_id)))
    conn.execute("INSERT INTO source_post_revisions(id,source_post_id,revision_no,source_content_hash,raw_text,sanitized_text) VALUES (?,?,?,?,?,?)", (revision_id,post_id,1,f'h{listing_id}','x','x'))
    facts = {'deal_type':'rent','monthly_rent_usd':800,'schema_version':'canonical_facts.v3','canonical_facts_hash':f'canon{listing_id}'}
    conn.execute("INSERT INTO canonical_records(id,source_post_id,source_post_revision_id,schema_version,parser_revision,facts_json,facts_hash,deal_type) VALUES (?,?,?,?,?,?,?,?)", (canonical_id,post_id,revision_id,'canonical_facts.v3','p',json.dumps(facts),f'canon{listing_id}','rent'))
    conn.execute("INSERT INTO v3_listings(id,public_listing_id,current_canonical_record_id,property_identity_key,project_name,property_type,public_location_display,layout,listing_status) VALUES (?,?,?,?,?,'公寓',?,'1房',?)", (listing_id,ql,canonical_id,f'prop-{listing_id}',project,location,status))
    conn.execute("INSERT INTO listing_offers(id,listing_id,canonical_record_id,offer_type,monthly_rent_usd,publication_policy) VALUES (?,?,?,'rent',800,'telegram_rent')", (offer_id,listing_id,canonical_id))
    if gallery is not None:
        package_id = f'PKG{listing_id}'
        conn.execute('''INSERT INTO v3_publication_packages(
            id,package_id,idempotency_key,listing_id,offer_id,canonical_record_id,package_version,target_channel_id,
            status,approval_mode,cover_path,gallery_json,caption_html,keyboard_json,content_hash,canonical_hash
        ) VALUES (?,?,?,?,?,?,1,'-100123','frozen','auto','cover.jpg',?,'ok','{}',?,?)''',
        (listing_id,package_id,package_id,listing_id,offer_id,canonical_id,json.dumps(list(gallery)),f'content-{listing_id}',f'canon{listing_id}'))
    conn.commit()


def test_start_home_and_reachable_services_match_locked_user_experience():
    screen = start_screen()
    assert screen['brand'] == '侨联小管家'
    labels = {label for row in HOME_ROWS for label, _ in row}
    assert labels == {'🔍 帮我找房','📅 我的预约','🛡 侨联保障','🛠 入住服务','房源频道','💬 联系我们'}
    assert {'property_coordination','life_services','nearby_needs','local:rfcity','lease','repair'} <= set(REACHABLE_SERVICES)


def test_ql_deep_links_and_frozen_three_button_compatibility():
    for action, prefix in [('details','property_details'),('photos','propertyphotos'),('book','property_book')]:
        payload = build_property_payload(action, 'QL000001')
        assert payload == f'{prefix}_QL000001'
        target = parse_start_payload(payload)
        assert target.action == action and target.public_listing_id == 'QL000001'
    assert parse_start_payload('details_QL000001').action == 'details'
    assert parse_start_payload('photos_QL000001').action == 'photos'
    assert parse_start_payload('book_QL000001').action == 'book'


def test_legacy_qc_qj_l_are_compat_only_and_new_business_never_generates_them():
    target = parse_start_payload('property_details_QC0350')
    assert target.public_listing_id is None and target.legacy.normalized_legacy_key == 'l_350'
    assert parse_start_payload('photos_QJ77').legacy.normalized_legacy_key == 'l_77'
    assert parse_start_payload('book_L12').legacy.normalized_legacy_key == 'l_12'
    with pytest.raises(ValueError, match='requires_ql'):
        require_ql_identity('QC0350')
    with pytest.raises(ValueError, match='requires_ql'):
        build_property_payload('details', 'QJ77')


def test_old_callback_compatibility_is_read_only_resolver():
    assert resolve_legacy_callback('details:QC0350').normalized_legacy_key == 'l_350'
    assert resolve_legacy_callback('photos|QJ77').action == 'photos'
    assert resolve_legacy_callback('book_L12').action == 'book'
    assert resolve_legacy_callback('details:QL000001') is None


def test_listing_status_policy_keeps_details_photos_contact_and_limits_booking():
    assert action_policy('active').book is True
    for status in ('pending','rented','inactive'):
        policy = action_policy(status)
        assert policy.details and policy.photos and policy.contact
        assert policy.book is False


def test_photos_are_current_approved_gallery_not_discussion():
    conn = db()
    gallery = ('approved-1.jpg','approved-2.jpg','approved-3.jpg','approved-4.jpg')
    seed_listing(conn, listing_id=1, ql='QL000001', gallery=gallery)
    service = PhotoService(UserListingRepository(conn))
    assert service.more_photos('QL000001') == gallery


def test_single_result_search_returns_similar_behavior():
    conn = db()
    seed_listing(conn, listing_id=1, ql='QL000001', project='富力城', location='BKK1')
    seed_listing(conn, listing_id=2, ql='QL000002', project='另一个公寓', location='BKK1')
    result = UserListingRepository(conn).search(query='富力城')
    assert len(result['results']) == 1
    assert result['results'][0].public_listing_id == 'QL000001'
    assert any(item.public_listing_id == 'QL000002' for item in result['similar'])


def test_appointment_date_time_submit_persist_status_sync_and_lease_reminder():
    conn = db()
    seed_listing(conn, listing_id=1, ql='QL000001')
    repo = AppointmentRepository(conn)
    flow = AppointmentFlow(repo)
    draft = flow.start(user_id=99, public_listing_id='QL000001')
    draft = flow.select_date(draft, '2026-09-12')
    draft = flow.select_time(draft, '15:30')
    appointment = flow.submit(draft, username='gym', display_name='Gym')
    assert appointment.public_listing_id == 'QL000001'
    assert appointment.status == 'pending'
    assert repo.list_for_user(99)[0].id == appointment.id
    assert repo.sync_status(appointment.id, 'confirmed').status == 'confirmed'
    conn.execute("INSERT INTO tenant_bindings(user_id,binding_code,property_name,lease_end_date) VALUES (99,'T1','富力城','2026-09-25')")
    reminder = repo.lease_reminders(99, today=date(2026,9,9), within_days=30)
    assert reminder[0]['days_remaining'] == 16


def test_service_routes_and_book_policy_are_preserved():
    conn = db()
    seed_listing(conn, listing_id=1, ql='QL000001', status='rented', gallery=('a','b','c','d'))
    service = UserBotService(UserListingRepository(conn), AppointmentRepository(conn))
    assert service.start().route == 'home'
    assert service.start('property_details_QL000001').route == 'details'
    assert service.start('propertyphotos_QL000001').payload['gallery'] == ('a','b','c','d')
    assert service.start('property_book_QL000001').payload['allowed'] is False
    assert {'contact_us','guarantee','move_in_services','lease','repair','property_coordination','life_services','nearby_needs','local:rfcity'} <= set(service.service_routes())


def test_user_bot_has_no_patch_monkey_patch_or_csv_excel_truth_dependency():
    text = '\n'.join(path.read_text(encoding='utf-8') for path in Path('qiaolian_v3/user_bot').glob('*.py'))
    lowered = text.lower()
    assert 'user_ux_patch' not in lowered
    assert 'attribution' not in lowered
    assert 'houses.csv' not in lowered
    assert '.xlsx' not in lowered
    assert 'openpyxl' not in lowered
