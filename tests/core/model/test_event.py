"""Event tests"""

import datetime as dt

from bemserver.core.model import Event
from bemserver.core.database import db


class TestEventModel:

    def test_event_open_extend_close(self, database):

        # open a new event
        ts_now = dt.datetime.now(dt.timezone.utc)
        evt_1 = Event.open("observation_missing", "src", "TIMESERIES", 42)
        assert evt_1.id is None
        assert evt_1.state == "NEW"
        assert evt_1.level == "ERROR"
        assert evt_1.timestamp_start is not None
        assert evt_1.timestamp_start > ts_now
        assert evt_1.timestamp_last_update == evt_1.timestamp_start
        assert evt_1.timestamp_end is None
        assert evt_1.description is None
        assert evt_1.duration == dt.timedelta(0)
        assert repr(evt_1) == (
            f"<{evt_1.__table__.name}>("
            f"id={evt_1.id}"
            f", category={evt_1.category}"
            f", level={evt_1.level}"
            f", timestamp_start={evt_1.timestamp_start}"
            f", timestamp_end={evt_1.timestamp_end}"
            f", duration={evt_1.duration}"
            f", source={evt_1.source}"
            f", target_type={evt_1.target_type}"
            f", target_id={evt_1.target_id}"
            f", state={evt_1.state}"
            f", timestamp_last_update={evt_1.timestamp_last_update}"
            f", description={evt_1.description}"
            ")")
        # until it is saved manually it does not exist in database
        evt_1.save(db)
        assert evt_1 is not None

        # OR open with timestamp start and auto save
        ts_start = dt.datetime.now(dt.timezone.utc)
        evt_2 = Event.open(
            "observation_missing", "src", "TIMESERIES", 42,
            timestamp_start=ts_start, db=db)
        assert evt_2.id is not None
        assert evt_2.timestamp_start == ts_start
        assert evt_2.timestamp_last_update > evt_2.timestamp_start
        assert evt_2.timestamp_end is None
        assert evt_2.duration == (
            evt_2.timestamp_last_update - evt_2.timestamp_start)

        # extend
        evt_1.extend()
        assert evt_1.state == "ONGOING"
        assert evt_1.timestamp_last_update > evt_1.timestamp_start
        assert evt_1.duration == (
            evt_1.timestamp_last_update - evt_1.timestamp_start)
        # and save changes manually
        evt_1.save(db)

        # OR extend and auto save
        evt_2.extend(db=db)
        assert evt_2.state == "ONGOING"
        assert evt_2.timestamp_last_update > evt_2.timestamp_start
        assert evt_2.duration == (
            evt_2.timestamp_last_update - evt_2.timestamp_start)

        # close
        evt_1.close()
        assert evt_1.state == "CLOSED"
        assert evt_1.timestamp_end is not None
        assert evt_1.timestamp_last_update == evt_1.timestamp_end
        assert evt_1.timestamp_last_update > evt_1.timestamp_start
        assert evt_1.duration == evt_1.timestamp_end - evt_1.timestamp_start
        # and save changes manually
        evt_1.save(db)

        # OR close and auto save
        evt_2.close(db=db)
        assert evt_2.state == "CLOSED"
        assert evt_2.timestamp_end is not None
        assert evt_2.timestamp_last_update == evt_2.timestamp_end
        assert evt_2.timestamp_last_update > evt_2.timestamp_start
        assert evt_2.duration == evt_2.timestamp_end - evt_2.timestamp_start

    def test_event_list_by_state(self, database):

        # no events at all
        evts = Event.list_by_state(db)
        assert evts == []
        evts = Event.list_by_state(db, states=("NEW",))
        assert evts == []
        evts = Event.list_by_state(db, states=("ONGOING",))
        assert evts == []
        evts = Event.list_by_state(db, states=("CLOSED",))
        assert evts == []

        # create 2 events
        evt_1 = Event.open(
            "observation_missing", "src", "TIMESERIES", 42, db=db)
        evt_2 = Event.open(
            "observation_missing", "src", "TIMESERIES", 69, db=db)

        # all events' state is NEW, so we have 2 events listing NEW or ONGOING
        evts = Event.list_by_state(db)
        assert evts == [(evt_1,), (evt_2,)]

        # close one event
        evt_2.close(db=db)
        # open and extend an event to ONGOING state
        evt_3 = Event.open(
            "observation_missing", "src", "TIMESERIES", 666, db=db)
        evt_3.extend(db=db)

        # 2 of 3 events are in NEW or ONGOING state
        evts = Event.list_by_state(db)
        assert evts == [(evt_1,), (evt_3,)]
        # one is NEW
        evts = Event.list_by_state(db, states=("NEW",))
        assert evts == [(evt_1,)]
        # one is ONGOING
        evts = Event.list_by_state(db, states=("ONGOING",))
        assert evts == [(evt_3,)]
        # one is closed
        evts = Event.list_by_state(db, states=("CLOSED",))
        assert evts == [(evt_2,)]
