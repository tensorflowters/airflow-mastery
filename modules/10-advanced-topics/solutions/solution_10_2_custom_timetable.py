"""
Solution 10.2: Custom Timetable
================================

A complete implementation of a business day timetable with:
- Weekday scheduling at 9:00 AM
- First Monday skip rule
- Last Friday extra 5:00 PM run
- Timezone awareness
- Proper serialization

This demonstrates Airflow 3's timetable extensibility for complex
scheduling requirements that go beyond cron expressions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pendulum
from pendulum import DateTime

from airflow.timetables.base import DagRunInfo, DataInterval, Timetable, TimeRestriction

if TYPE_CHECKING:
    pass


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def get_first_monday(year: int, month: int) -> DateTime:
    """
    Get the first Monday of a given month.

    Args:
        year: The year
        month: The month (1-12)

    Returns:
        DateTime of the first Monday at midnight
    """
    first_day = pendulum.datetime(year, month, 1)

    # Monday is weekday 0 in pendulum/Python
    if first_day.weekday() == 0:
        return first_day

    # Calculate days until next Monday
    days_until_monday = (7 - first_day.weekday()) % 7
    return first_day.add(days=days_until_monday)


def get_last_friday(year: int, month: int) -> DateTime:
    """
    Get the last Friday of a given month.

    Args:
        year: The year
        month: The month (1-12)

    Returns:
        DateTime of the last Friday at midnight
    """
    # Go to last day of month
    last_day = pendulum.datetime(year, month, 1).end_of("month").start_of("day")

    # Friday is weekday 4 in pendulum/Python
    days_since_friday = (last_day.weekday() - 4) % 7
    return last_day.subtract(days=days_since_friday)


def is_weekend(dt: DateTime) -> bool:
    """
    Check if a date is a weekend.

    Saturday = 5, Sunday = 6 in Python's weekday numbering.
    """
    return dt.weekday() >= 5


def next_weekday(dt: DateTime) -> DateTime:
    """
    Get the next weekday, skipping weekends.

    If Saturday, skip to Monday (+2 days).
    If Sunday, skip to Monday (+1 day).
    Otherwise, move to next day.
    """
    next_day = dt.add(days=1)

    if next_day.weekday() == 5:  # Saturday
        return next_day.add(days=2)
    elif next_day.weekday() == 6:  # Sunday
        return next_day.add(days=1)

    return next_day


# =============================================================================
# CUSTOM TIMETABLE
# =============================================================================


class BusinessDayTimetable(Timetable):
    """
    Custom timetable for business day scheduling.

    Schedule Rules:
    1. Every weekday at 9:00 AM
    2. Skip the first Monday of each month
    3. Add an extra run at 5:00 PM on the last Friday of each month

    This timetable demonstrates:
    - Complex scheduling logic
    - Month-relative date calculations
    - Multiple run times on special days
    - Timezone handling
    - Proper serialization for database storage

    Example Usage:
        @dag(
            timetable=BusinessDayTimetable(timezone="America/New_York"),
            start_date=datetime(2024, 1, 1),
        )
        def my_business_dag():
            ...
    """

    # Schedule constants
    MORNING_HOUR = 9
    MORNING_MINUTE = 0
    EVENING_HOUR = 17
    EVENING_MINUTE = 0

    def __init__(self, timezone: str = "America/New_York"):
        """
        Initialize the business day timetable.

        Args:
            timezone: The business timezone for scheduling.
                     All run times will be calculated in this timezone.
        """
        self.timezone = timezone
        self._tz = pendulum.timezone(timezone)

    # =========================================================================
    # SERIALIZATION
    # =========================================================================

    def serialize(self) -> dict:
        """
        Serialize timetable configuration for database storage.

        Airflow stores timetable configuration in the database,
        so all parameters must be serializable.
        """
        return {"timezone": self.timezone}

    @classmethod
    def deserialize(cls, data: dict) -> "BusinessDayTimetable":
        """
        Deserialize timetable from database storage.

        Args:
            data: Dictionary with serialized configuration

        Returns:
            Reconstructed timetable instance
        """
        return cls(timezone=data["timezone"])

    # =========================================================================
    # SCHEDULE LOGIC
    # =========================================================================

    def _is_first_monday(self, dt: DateTime) -> bool:
        """
        Check if date is the first Monday of its month.

        Args:
            dt: Date to check

        Returns:
            True if this is the first Monday of the month
        """
        first_monday = get_first_monday(dt.year, dt.month)
        return (
            dt.year == first_monday.year
            and dt.month == first_monday.month
            and dt.day == first_monday.day
        )

    def _is_last_friday(self, dt: DateTime) -> bool:
        """
        Check if date is the last Friday of its month.

        Args:
            dt: Date to check

        Returns:
            True if this is the last Friday of the month
        """
        last_friday = get_last_friday(dt.year, dt.month)
        return (
            dt.year == last_friday.year
            and dt.month == last_friday.month
            and dt.day == last_friday.day
        )

    def _should_skip_morning(self, dt: DateTime) -> bool:
        """
        Determine if a morning run should be skipped.

        Skip morning runs on:
        - Weekends
        - First Monday of the month

        Args:
            dt: Date to check

        Returns:
            True if the morning run should be skipped
        """
        if is_weekend(dt):
            return True

        if self._is_first_monday(dt):
            return True

        return False

    def _create_morning_time(self, dt: DateTime) -> DateTime:
        """Create a DateTime at 9:00 AM on the given date."""
        return dt.set(
            hour=self.MORNING_HOUR,
            minute=self.MORNING_MINUTE,
            second=0,
            microsecond=0,
        )

    def _create_evening_time(self, dt: DateTime) -> DateTime:
        """Create a DateTime at 5:00 PM on the given date."""
        return dt.set(
            hour=self.EVENING_HOUR,
            minute=self.EVENING_MINUTE,
            second=0,
            microsecond=0,
        )

    def _get_next_morning_run(self, after: DateTime) -> DateTime:
        """
        Get the next valid 9 AM run after the given time.

        This handles:
        - Moving to next day if past 9 AM
        - Skipping weekends
        - Skipping first Monday of month

        Args:
            after: Find next run after this time

        Returns:
            DateTime of next valid morning run
        """
        # Start from the given time in our timezone
        current = after.in_timezone(self._tz)

        # If we're past 9 AM today, start from tomorrow
        if current.hour >= self.MORNING_HOUR:
            candidate = current.add(days=1)
        else:
            candidate = current

        candidate = self._create_morning_time(candidate)

        # Find the next valid weekday that's not first Monday
        max_iterations = 10  # Safety limit
        for _ in range(max_iterations):
            if not self._should_skip_morning(candidate):
                return candidate

            # Move to next day
            candidate = self._create_morning_time(candidate.add(days=1))

            # Skip weekend in one jump
            if candidate.weekday() == 5:  # Saturday
                candidate = self._create_morning_time(candidate.add(days=2))
            elif candidate.weekday() == 6:  # Sunday
                candidate = self._create_morning_time(candidate.add(days=1))

        # Fallback (should never reach here)
        return candidate

    def _get_next_evening_run(self, after: DateTime) -> DateTime | None:
        """
        Get the next 5 PM run on last Friday, if within reasonable range.

        Args:
            after: Find next run after this time

        Returns:
            DateTime of next last-Friday evening run
        """
        current = after.in_timezone(self._tz)

        # Check this month's last Friday
        last_friday = get_last_friday(current.year, current.month)
        last_friday_evening = self._create_evening_time(last_friday)

        if last_friday_evening > current:
            return last_friday_evening

        # Check next month's last Friday
        if current.month == 12:
            next_month = pendulum.datetime(current.year + 1, 1, 1)
        else:
            next_month = pendulum.datetime(current.year, current.month + 1, 1)

        last_friday = get_last_friday(next_month.year, next_month.month)
        return self._create_evening_time(last_friday)

    def _get_next_run_time(self, after: DateTime) -> DateTime:
        """
        Get the next scheduled run time after the given time.

        Returns whichever comes first:
        - Next valid 9 AM weekday run
        - Next 5 PM last-Friday run

        Args:
            after: Find next run after this time

        Returns:
            DateTime of next run
        """
        next_morning = self._get_next_morning_run(after)
        next_evening = self._get_next_evening_run(after)

        # Return the earlier of the two
        if next_evening and next_evening < next_morning:
            return next_evening

        return next_morning

    # =========================================================================
    # TIMETABLE INTERFACE
    # =========================================================================

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        """
        Calculate the next DAG run info.

        This is the main method Airflow calls to determine when
        the next automated DAG run should be scheduled.

        Args:
            last_automated_data_interval: The data interval of the last
                automated run, or None if this is the first run.
            restriction: Time restrictions including:
                - earliest: Earliest allowed run time (from start_date)
                - latest: Latest allowed run time (from end_date)
                - catchup: Whether catchup is enabled

        Returns:
            DagRunInfo containing the next run's data interval,
            or None if there should be no more runs.
        """
        # Determine starting point
        if last_automated_data_interval is not None:
            # Continue from last run
            start_after = last_automated_data_interval.end
        elif restriction.earliest is not None:
            # First run - start from earliest allowed
            start_after = restriction.earliest
        else:
            # No restrictions - shouldn't happen in practice
            return None

        # Find next valid run time
        next_run = self._get_next_run_time(start_after)

        # Check against end restriction
        if restriction.latest is not None and next_run > restriction.latest:
            return None

        # For point-in-time schedules, data interval is the run time itself
        data_interval = DataInterval(start=next_run, end=next_run)

        return DagRunInfo.interval(data_interval=data_interval)

    def infer_manual_data_interval(
        self,
        *,
        run_after: DateTime,
    ) -> DataInterval:
        """
        Infer data interval for a manual trigger.

        When a user manually triggers a DAG run, we need to
        determine what data interval it represents.

        For this point-in-time schedule, we use the trigger
        time as both start and end of the interval.

        Args:
            run_after: When the manual run was triggered

        Returns:
            DataInterval for the manual run
        """
        # Normalize to our timezone
        run_time = run_after.in_timezone(self._tz)

        return DataInterval(start=run_time, end=run_time)

    # =========================================================================
    # UI DISPLAY
    # =========================================================================

    @property
    def summary(self) -> str:
        """
        Human-readable summary for Airflow UI.

        This appears in the DAG details page and helps users
        understand the scheduling logic at a glance.
        """
        return (
            f"Business days @ {self.MORNING_HOUR}AM "
            f"(skip 1st Mon, +{self.EVENING_HOUR - 12}PM last Fri) "
            f"[{self.timezone}]"
        )

    def __repr__(self) -> str:
        """Debug representation."""
        return f"BusinessDayTimetable(timezone={self.timezone!r})"

    def __eq__(self, other) -> bool:
        """Equality comparison for testing."""
        if not isinstance(other, BusinessDayTimetable):
            return False
        return self.timezone == other.timezone

    def __hash__(self) -> int:
        """Hash for dictionary usage."""
        return hash(self.timezone)


# =============================================================================
# DAG USING THE TIMETABLE
# =============================================================================


from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="solution_10_2_custom_timetable",
    timetable=BusinessDayTimetable(timezone="America/New_York"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["solution", "timetable", "scheduling"],
    doc_md="""
    ## Business Day Processing DAG

    This DAG demonstrates a custom timetable with complex scheduling:

    - **Regular runs**: Every weekday at 9:00 AM
    - **Skip rule**: First Monday of each month (planning meetings)
    - **Extra runs**: Last Friday at 5:00 PM (month-end processing)

    ### Schedule Examples

    | Date | Time | Status |
    |------|------|--------|
    | Mon Jan 1 | 9 AM | SKIP (first Monday) |
    | Tue Jan 2 | 9 AM | RUN |
    | Fri Jan 26 | 9 AM | RUN |
    | Fri Jan 26 | 5 PM | RUN (month-end) |
    | Sat Jan 27 | - | SKIP (weekend) |
    """,
)
def business_day_processing():
    """
    Business day processing DAG with custom scheduling.

    Uses BusinessDayTimetable for complex scheduling logic
    that cannot be expressed with cron.
    """

    @task
    def identify_run_type(**context) -> dict:
        """
        Identify what type of business run this is.

        Returns dict with run classification.
        """
        import logging

        logger = logging.getLogger(__name__)
        logical_date = context["logical_date"]

        run_info = {
            "logical_date": str(logical_date),
            "is_morning_run": logical_date.hour == 9,
            "is_evening_run": logical_date.hour == 17,
            "is_month_end": False,
        }

        # Check if this is a month-end run
        if logical_date.hour == 17:
            last_friday = get_last_friday(logical_date.year, logical_date.month)
            if logical_date.day == last_friday.day:
                run_info["is_month_end"] = True
                logger.info("This is a MONTH-END processing run")

        logger.info(f"Run classification: {run_info}")
        return run_info

    @task
    def process_data(run_info: dict) -> dict:
        """
        Process business data based on run type.

        Different processing for morning vs month-end runs.
        """
        import logging

        logger = logging.getLogger(__name__)

        if run_info.get("is_month_end"):
            logger.info("Executing month-end processing...")
            return {
                "process_type": "month_end",
                "records_processed": 50000,
                "reports_generated": ["monthly_summary", "kpi_dashboard"],
            }
        else:
            logger.info("Executing daily processing...")
            return {
                "process_type": "daily",
                "records_processed": 1000,
                "reports_generated": ["daily_summary"],
            }

    @task
    def send_notifications(run_info: dict, process_result: dict) -> None:
        """Send appropriate notifications based on run type."""
        import logging

        logger = logging.getLogger(__name__)

        if run_info.get("is_month_end"):
            logger.info(
                f"Sending month-end notifications: "
                f"{process_result['records_processed']} records processed"
            )
            # In production: send to finance team, management
        else:
            logger.info("Daily processing complete, minimal notifications")

    # Build the pipeline
    run_info = identify_run_type()
    process_result = process_data(run_info)
    send_notifications(run_info, process_result)


# Instantiate the DAG
business_day_processing()


# =============================================================================
# TESTING
# =============================================================================


def test_timetable():
    """Comprehensive tests for BusinessDayTimetable."""
    print("=" * 60)
    print("Testing BusinessDayTimetable")
    print("=" * 60)

    tt = BusinessDayTimetable(timezone="America/New_York")

    # Test helper functions
    print("\n--- Helper Function Tests ---")

    # January 2024: Jan 1 is a Monday
    first_mon_jan = get_first_monday(2024, 1)
    print(f"First Monday Jan 2024: {first_mon_jan.date()}")
    assert first_mon_jan.day == 1, "Jan 1, 2024 is a Monday"

    # February 2024: Feb 1 is a Thursday
    first_mon_feb = get_first_monday(2024, 2)
    print(f"First Monday Feb 2024: {first_mon_feb.date()}")
    assert first_mon_feb.day == 5, "First Monday in Feb 2024 is the 5th"

    # Last Fridays
    last_fri_jan = get_last_friday(2024, 1)
    print(f"Last Friday Jan 2024: {last_fri_jan.date()}")
    assert last_fri_jan.day == 26

    last_fri_feb = get_last_friday(2024, 2)
    print(f"Last Friday Feb 2024: {last_fri_feb.date()}")
    assert last_fri_feb.day == 23

    # Test skip logic
    print("\n--- Skip Logic Tests ---")

    tz = pendulum.timezone("America/New_York")

    # First Monday should be skipped
    first_monday = pendulum.datetime(2024, 1, 1, 9, 0, tz=tz)
    assert tt._is_first_monday(first_monday), "Jan 1 is first Monday"
    assert tt._should_skip_morning(first_monday), "First Monday should be skipped"
    print(f"✓ First Monday (Jan 1): correctly skipped")

    # Regular Tuesday should run
    tuesday = pendulum.datetime(2024, 1, 2, 9, 0, tz=tz)
    assert not tt._should_skip_morning(tuesday), "Tuesday should run"
    print(f"✓ Regular Tuesday (Jan 2): correctly scheduled")

    # Saturday should be skipped
    saturday = pendulum.datetime(2024, 1, 6, 9, 0, tz=tz)
    assert tt._should_skip_morning(saturday), "Saturday should be skipped"
    print(f"✓ Saturday (Jan 6): correctly skipped")

    # Last Friday detection
    last_friday = pendulum.datetime(2024, 1, 26, 9, 0, tz=tz)
    assert tt._is_last_friday(last_friday), "Jan 26 is last Friday"
    print(f"✓ Last Friday (Jan 26): correctly detected")

    # Test next run calculations
    print("\n--- Next Run Calculation Tests ---")

    # After first Monday morning -> should be Tuesday
    after_first_mon = pendulum.datetime(2024, 1, 1, 10, 0, tz=tz)
    next_run = tt._get_next_morning_run(after_first_mon)
    print(f"After Jan 1 10AM: next morning run = {next_run}")
    assert next_run.day == 2, "Should skip to Tuesday Jan 2"

    # Friday afternoon -> should be Monday (unless first Monday)
    friday_afternoon = pendulum.datetime(2024, 1, 5, 15, 0, tz=tz)
    next_run = tt._get_next_morning_run(friday_afternoon)
    print(f"After Jan 5 3PM: next morning run = {next_run}")
    assert next_run.day == 8, "Should be Monday Jan 8"

    # Test combined next run (morning vs evening)
    print("\n--- Combined Run Tests ---")

    # Day before last Friday, after morning run
    jan_25_afternoon = pendulum.datetime(2024, 1, 25, 15, 0, tz=tz)
    next_combined = tt._get_next_run_time(jan_25_afternoon)
    print(f"After Jan 25 3PM: next run = {next_combined}")
    # Next morning is Jan 26 9AM, but evening run Jan 26 5PM is later
    assert next_combined.day == 26
    assert next_combined.hour == 9

    # After last Friday morning -> evening run same day
    jan_26_morning_after = pendulum.datetime(2024, 1, 26, 10, 0, tz=tz)
    next_combined = tt._get_next_run_time(jan_26_morning_after)
    print(f"After Jan 26 10AM: next run = {next_combined}")
    # Evening run at 5PM should come before next morning run
    assert next_combined.day == 26
    assert next_combined.hour == 17

    # Test serialization
    print("\n--- Serialization Tests ---")

    serialized = tt.serialize()
    print(f"Serialized: {serialized}")
    assert serialized == {"timezone": "America/New_York"}

    deserialized = BusinessDayTimetable.deserialize(serialized)
    assert deserialized.timezone == tt.timezone
    print("✓ Serialization round-trip successful")

    # Test UI summary
    print("\n--- UI Display Tests ---")
    print(f"Summary: {tt.summary}")
    print(f"Repr: {repr(tt)}")

    print("\n" + "=" * 60)
    print("All tests passed! ✓")
    print("=" * 60)


if __name__ == "__main__":
    test_timetable()
