from __future__ import annotations

import json
import sqlite3
from datetime import date
from pathlib import Path

import pytest

from qiaolian_v3.db.migration_runner import MigrationRunner
from qiaolian_v3.legacy.callback_compat import resolve_legacy_callback
from qiaolian_v3.listing.public_id import normalize_public_id
from qiaolian_v3.user_bot import HOME_ROWS, REACHABLE_SERVICES, start_screen
from qiaolian_v3.user_bot.deeplinks import build_property_payload, parse_start_payload, require_ql_identity
from qiaolian_v3.user_bot.listing_actions import action_policy
from qiaolian_v3.user_bot.photos import PhotoService
from qiaolian_v3.user_bot.repository import AppointmentRepository, UserListingRepository
from qiaolian_v3.user_bot.router import UserBotHandler, UserBotRouter, UserUpdate
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
    ql = require_ql_identity(ql)
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


def test_formal_ql_contract_uses_single_normalizer_and_rejects_old_numeric_ids():
    assert require_ql_identity('ql-rf-a2b3') == 'QL-RF-A2B3'
    assert require_ql_identity('QL-BK-C3D4') == 'QL-BK-C3D4'
    assert normalize_public_id('QL-RF-A2B3') == require_ql_identity('QL-RF-A2B3')
    for invalid in ('QL000001', 'QL000101', 'QC0350', 'QJ77', 'L12'):
        with pytest.raises(ValueError, match='requires_ql'):
            require_ql_identity(invalid)


def test_formal_property_deep_links_and_phase6_three_button_compatibility():
    ql = 'QL-RF-A2B3'
    for action in ('details', 'photos', 'book'):
        payload = build_property_payload(action, ql)
        assert payload == f'property_{ql}_{action}'
        target = parse_start_payload(payload)
        assert target.action == action and target.public_listing_id == ql
        frozen = parse_start_payload(f'{action}_{ql}')
        assert frozen.action == action and frozen.public_listing_id == ql
    assert parse_start_payload('property_QL000001_details') is None
    assert parse_start_payload('details_QL000001') is None


def test_legacy_qc_qj_l_are_compat_only_and_new_business_never_generates_them():
    target = parse_start_payload('property_details_QC0350')
    assert target.public_listing_id is None and target.legacy.normalized_legacy_key == 'l_350'
    assert parse_start_payload('photos_QJ77').legacy.normalized_legacy_key == 'l_77'
    assert parse_start_payload('book_L12').legacy.normalized_legacy_key == 'l_12'
    with pytest.raises(ValueError, match='requires_ql'):
        build_property_payload('details', 'QJ77')


def test_old_callback_compatibility_is_read_only_resolver():
    assert resolve_legacy_callback('details:QC0350').normalized_legacy_key == 'l_350'
    assert resolve_legacy_callback('photos|QJ77').action == 'photos'
    assert resolve_legacy_callback('book_L12').action == 'book'
    assert resolve_legacy_callback('details:QL-RF-A2B3') is None


def test_listing_status_policy_keeps_details_photos_contact_and_limits_booking():
    assert action_policy('active').book is True
    for status in ('pending','rented','inactive'):
        policy = action_policy(status)
        assert policy.details and policy.photos and policy.contact
        assert policy.book is False


def test_photos_are_current_approved_gallery_not_discussion():
    conn = db()
    gallery = ('approved-1.jpg','approved-2.jpg','approved-3.jpg','approved-4.jpg')
    seed_listing(conn, listing_id=1, ql='QL-RF-A2B3', gallery=gallery)
    service = PhotoService(UserListingRepository(conn))
    assert service.more_photos('QL-RF-A2B3') == gallery


def test_single_result_search_returns_similar_behavior():
    conn = db()
    seed_listing(conn, listing_id=1, ql='QL-RF-A2B3', project='富力城', location='BKK1')
    seed_listing(conn, listing_id=2, ql='QL-BK-C3D4', project='另一个公寓', location='BKK1')
    result = UserListingRepository(conn).search(query='富力城')
    assert len(result['results']) == 1
    assert result['results'][0].public_listing_id == 'QL-RF-A2B3'
    assert any(item.public_listing_id == 'QL-BK-C3D4' for item in result['similar'])


def test_user_handler_routes_home_find_services_and_real_appointment_callback_flow():
    conn = db()
    gallery = ('approved-1.jpg','approved-2.jpg','approved-3.jpg','approved-4.jpg')
    seed_listing(conn, listing_id=1, ql='QL-RF-A2B3', gallery=gallery)
    service = UserBotService(UserListingRepository(conn), AppointmentRepository(conn))
    handler = UserBotHandler(UserBotRouter(service))

    assert handler.handle(UserUpdate(user_id=99, callback_data='home')).route == 'home'
    found = handler.handle(UserUpdate(user_id=99, callback_data='find_home', data={'query':'富力城'}))
    assert found.route == 'find_home' and len(found.payload['results']) == 1
    details = handler.handle(UserUpdate(user_id=99, callback_data='details:QL-RF-A2B3'))
    assert details.route == 'details'
    photos = handler.handle(UserUpdate(user_id=99, callback_data='photos:QL-RF-A2B3'))
    assert photos.route == 'photos' and photos.payload['gallery'] == gallery

    step = handler.handle(UserUpdate(user_id=99, callback_data='book:QL-RF-A2B3'))
    assert step.route == 'appointment_date'
    step = handler.handle(UserUpdate(user_id=99, callback_data='appointment_date:2026-09-12'))
    assert step.route == 'appointment_time'
    step = handler.handle(UserUpdate(user_id=99, callback_data='appointment_time:15:30'))
    assert step.route == 'appointment_submit'
    done = handler.handle(UserUpdate(
        user_id=99, callback_data='appointment_submit', data={'username':'gym','display_name':'Gym'}
    ))
    assert done.route == 'appointment_confirmed'
    assert conn.execute('SELECT listing_id,appointment_date,appointment_time FROM appointments').fetchone()[:] == ('QL-RF-A2B3','2026-09-12','15:30')
    assert handler.handle(UserUpdate(user_id=99, callback_data='my_appointments')).payload[0].public_listing_id == 'QL-RF-A2B3'

    for callback in ('contact_us','guarantee','move_in_services','lease','repair','property_coordination','life_services','nearby_needs','local:rfcity'):
        assert handler.handle(UserUpdate(user_id=99, callback_data=callback)).route == callback


def test_start_deep_link_enters_router_booking_flow():
    conn = db()
    seed_listing(conn, listing_id=1, ql='QL-RF-A2B3')
    handler = UserBotHandler(UserBotRouter(UserBotService(UserListingRepository(conn), AppointmentRepository(conn))))
    result = handler.handle(UserUpdate(user_id=7, start_payload='property_QL-RF-A2B3_book'))
    assert result.route == 'appointment_date'


def test_appointment_status_sync_and_lease_reminder_are_preserved():
    conn = db()
    seed_listing(conn, listing_id=1, ql='QL-RF-A2B3')
    repo = AppointmentRepository(conn)
    appointment = repo.create(
        user_id=99, username='gym', display_name='Gym', public_listing_id='QL-RF-A2B3',
        appointment_date='2026-09-12', appointment_time='15:30',
    )
    assert repo.sync_status(appointment.id, 'confirmed').status == 'confirmed'
    conn.execute("INSERT INTO tenant_bindings(user_id,binding_code,property_name,lease_end_date) VALUES (99,'T1','富力城','2026-09-25')")
    reminder = repo.lease_reminders(99, today=date(2026,9,9), within_days=30)
    assert reminder[0]['days_remaining'] == 16


def test_user_bot_has_no_patch_monkey_patch_or_csv_excel_truth_dependency():
    text = '\n'.join(path.read_text(encoding='utf-8') for path in Path('qiaolian_v3/user_bot').glob('*.py'))
    lowered = text.lower()
    assert 'user_ux_patch' not in lowered
    assert 'attribution' not in lowered
    assert 'houses.csv' not in lowered
    assert '.xlsx' not in lowered
    assert 'openpyxl' not in lowered
