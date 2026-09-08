from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timezone

from .deeplinks import require_ql_identity


@dataclass(frozen=True)
class ListingView:
    listing_id: int
    public_listing_id: str
    listing_status: str
    project_name: str
    property_type: str
    layout: str
    public_location_display: str
    offer_id: int | None
    monthly_rent_usd: int | None


@dataclass(frozen=True)
class AppointmentView:
    id: int
    user_id: int
    public_listing_id: str
    appointment_date: str
    appointment_time: str
    status: str


class UserListingRepository:
    """V3 user reads use QL/listing/package truth only; no CSV/Excel fallback."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def get_by_public_id(self, public_listing_id: str) -> ListingView:
        ql = require_ql_identity(public_listing_id)
        row = self.conn.execute(
            '''SELECT l.*,o.id AS offer_id,o.monthly_rent_usd
               FROM v3_listings l
               LEFT JOIN listing_offers o
                 ON o.listing_id=l.id AND o.offer_type='rent' AND o.is_current=1
               WHERE l.public_listing_id=?''',
            (ql,),
        ).fetchone()
        if row is None:
            raise LookupError('listing_not_found')
        return ListingView(
            listing_id=int(row['id']), public_listing_id=str(row['public_listing_id']),
            listing_status=str(row['listing_status']), project_name=str(row['project_name']),
            property_type=str(row['property_type']), layout=str(row['layout']),
            public_location_display=str(row['public_location_display']),
            offer_id=int(row['offer_id']) if row['offer_id'] is not None else None,
            monthly_rent_usd=int(row['monthly_rent_usd']) if row['monthly_rent_usd'] is not None else None,
        )

    def approved_gallery(self, public_listing_id: str) -> tuple[str, ...]:
        listing = self.get_by_public_id(public_listing_id)
        row = self.conn.execute(
            '''SELECT p.gallery_json
               FROM v3_publication_packages p
               LEFT JOIN v3_channel_posts c ON c.current_package_id=p.id
               WHERE p.listing_id=? AND p.offer_id=?
                 AND p.status IN ('frozen','published')
               ORDER BY CASE WHEN c.id IS NOT NULL THEN 0 ELSE 1 END,p.id DESC
               LIMIT 1''',
            (listing.listing_id, listing.offer_id),
        ).fetchone()
        if row is None:
            return ()
        return tuple(str(value) for value in json.loads(str(row['gallery_json'])) if str(value))

    def search(self, *, query: str, limit: int = 10) -> dict[str, object]:
        needle = f"%{str(query).strip()}%"
        rows = self.conn.execute(
            '''SELECT public_listing_id FROM v3_listings
               WHERE listing_status IN ('active','pending','rented','inactive')
                 AND (project_name LIKE ? OR public_location_display LIKE ? OR layout LIKE ?)
               ORDER BY CASE listing_status WHEN 'active' THEN 0 ELSE 1 END,id DESC LIMIT ?''',
            (needle, needle, needle, max(1, int(limit))),
        ).fetchall()
        results = tuple(self.get_by_public_id(str(row['public_listing_id'])) for row in rows)
        similar: tuple[ListingView, ...] = ()
        if len(results) == 1:
            base = results[0]
            rows2 = self.conn.execute(
                '''SELECT public_listing_id FROM v3_listings
                   WHERE id<>? AND listing_status='active'
                     AND (public_location_display=? OR property_type=?)
                   ORDER BY id DESC LIMIT 3''',
                (base.listing_id, base.public_location_display, base.property_type),
            ).fetchall()
            similar = tuple(self.get_by_public_id(str(row['public_listing_id'])) for row in rows2)
        return {'results': results, 'similar': similar}


class AppointmentRepository:
    """Adapter over the existing production appointments/tenant_bindings persistence."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def create(
        self,
        *, user_id: int, username: str, display_name: str, public_listing_id: str,
        appointment_date: str, appointment_time: str, contact_value: str = '', note: str = '',
    ) -> AppointmentView:
        ql = require_ql_identity(public_listing_id)
        cur = self.conn.execute(
            '''INSERT INTO appointments(
                user_id,username,display_name,listing_id,viewing_mode,appointment_date,
                appointment_time,contact_value,note,status,created_at
            ) VALUES (?,?,?,?,'onsite',?,?,?,?, 'pending',?)''',
            (
                int(user_id), str(username), str(display_name), ql,
                str(appointment_date), str(appointment_time), str(contact_value), str(note),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.conn.commit()
        return self.get(int(cur.lastrowid))

    def get(self, appointment_id: int) -> AppointmentView:
        row = self.conn.execute('SELECT * FROM appointments WHERE id=?', (int(appointment_id),)).fetchone()
        if row is None:
            raise LookupError('appointment_not_found')
        return AppointmentView(
            id=int(row['id']), user_id=int(row['user_id']), public_listing_id=str(row['listing_id']),
            appointment_date=str(row['appointment_date']), appointment_time=str(row['appointment_time']),
            status=str(row['status']),
        )

    def list_for_user(self, user_id: int) -> tuple[AppointmentView, ...]:
        rows = self.conn.execute('SELECT id FROM appointments WHERE user_id=? ORDER BY id DESC', (int(user_id),)).fetchall()
        return tuple(self.get(int(row['id'])) for row in rows)

    def sync_status(self, appointment_id: int, status: str) -> AppointmentView:
        allowed = {'pending', 'confirmed', 'completed', 'cancelled', 'rescheduled'}
        if str(status) not in allowed:
            raise ValueError('invalid_appointment_status')
        cur = self.conn.execute('UPDATE appointments SET status=? WHERE id=?', (str(status), int(appointment_id)))
        if cur.rowcount != 1:
            raise LookupError('appointment_not_found')
        self.conn.commit()
        return self.get(appointment_id)

    def lease_reminders(self, user_id: int, *, today: date | None = None, within_days: int = 30) -> tuple[dict[str, object], ...]:
        today = today or date.today()
        rows = self.conn.execute(
            '''SELECT binding_code,property_name,lease_end_date FROM tenant_bindings
               WHERE user_id=? AND lease_end_date<>'' ORDER BY lease_end_date''',
            (int(user_id),),
        ).fetchall()
        reminders: list[dict[str, object]] = []
        for row in rows:
            try:
                end = date.fromisoformat(str(row['lease_end_date']))
            except ValueError:
                continue
            days = (end - today).days
            if 0 <= days <= int(within_days):
                reminders.append({
                    'binding_code': str(row['binding_code']),
                    'property_name': str(row['property_name']),
                    'lease_end_date': end.isoformat(),
                    'days_remaining': days,
                })
        return tuple(reminders)


__all__ = ['AppointmentRepository', 'AppointmentView', 'ListingView', 'UserListingRepository']
