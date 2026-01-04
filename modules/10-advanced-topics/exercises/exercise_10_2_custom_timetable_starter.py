"""
Exercise 10.2: Custom Timetable (Starter)
==========================================

Create a custom timetable for complex business scheduling.

Schedule Requirements:
1. Every weekday at 9:00 AM
2. Skip first Monday of each month
3. Extra run on last Friday at 5:00 PM

Requirements:
- pendulum library (included with Airflow)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pendulum
from pendulum import DateTime

from airflow.timetables.base import DagRunInfo, DataInterval, Timetable, TimeRestriction

if TYPE_CHECKING:
    from pendulum import DateTime


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
        DateTime of the first Monday
    """
    # TODO: Implement this function
    # Hint: Start from the 1st of the month and find the next Monday
    pass


def get_last_friday(year: int, month: int) -> DateTime:
    """
    Get the last Friday of a given month.

    Args:
        year: The year
        month: The month (1-12)

    Returns:
        DateTime of the last Friday
    """
    # TODO: Implement this function
    # Hint: Start from the end of the month and work backwards
    pass


def is_weekend(dt: DateTime) -> bool:
    """Check if a date is a weekend (Saturday=5, Sunday=6)."""
    # TODO: Implement this function
    pass


def next_weekday(dt: DateTime) -> DateTime:
    """Get the next weekday (skip weekends)."""
    # TODO: Implement this function
    pass


# =============================================================================
# CUSTOM TIMETABLE
# =============================================================================


class BusinessDayTimetable(Timetable):
    """
    Custom timetable for business day scheduling.

    Schedule:
    - Every weekday at 9:00 AM
    - Skip first Monday of each month
    - Extra run on last Friday at 5:00 PM

    TODO: Implement all methods marked with TODO.
    """

    # Time constants
    MORNING_HOUR = 9
    EVENING_HOUR = 17  # 5:00 PM

    def __init__(self, timezone: str = "America/New_York"):
        """
        Initialize the timetable.

        Args:
            timezone: Business timezone for scheduling
        """
        self.timezone = timezone
        self._tz = pendulum.timezone(timezone)

    # =========================================================================
    # SERIALIZATION (Required for database storage)
    # =========================================================================

    def serialize(self) -> dict:
        """Serialize timetable configuration for database storage."""
        # TODO: Return dict with timezone
        pass

    @classmethod
    def deserialize(cls, data: dict) -> "BusinessDayTimetable":
        """Deserialize timetable from database."""
        # TODO: Reconstruct timetable from serialized data
        pass

    # =========================================================================
    # SCHEDULE LOGIC
    # =========================================================================

    def _is_first_monday(self, dt: DateTime) -> bool:
        """Check if date is the first Monday of its month."""
        # TODO: Implement check using get_first_monday
        pass

    def _is_last_friday(self, dt: DateTime) -> bool:
        """Check if date is the last Friday of its month."""
        # TODO: Implement check using get_last_friday
        pass

    def _should_skip(self, dt: DateTime) -> bool:
        """
        Determine if a date should be skipped.

        Skip if:
        - Weekend
        - First Monday of month
        """
        # TODO: Implement skip logic
        pass

    def _get_next_morning_run(self, after: DateTime) -> DateTime:
        """
        Get the next valid 9 AM run after the given time.

        Args:
            after: Find next run after this time

        Returns:
            DateTime of next valid morning run
        """
        # TODO: Implement logic to find next valid 9 AM run
        # Consider:
        # 1. Move to next day
        # 2. Set time to 9 AM
        # 3. Skip weekends
        # 4. Skip first Monday
        pass

    def _get_next_evening_run(self, after: DateTime) -> DateTime | None:
        """
        Get the next 5 PM run on last Friday, if applicable.

        Args:
            after: Find next run after this time

        Returns:
            DateTime of next last-Friday evening run, or None
        """
        # TODO: Implement logic to find next last-Friday 5 PM run
        pass

    def _get_next_run_time(self, after: DateTime) -> DateTime:
        """
        Get the next scheduled run time after the given time.

        This should return whichever comes first:
        - Next valid 9 AM weekday run
        - Next 5 PM last-Friday run

        Args:
            after: Find next run after this time

        Returns:
            DateTime of next run
        """
        # TODO: Implement combined logic
        pass

    # =========================================================================
    # TIMETABLE INTERFACE (Required by Airflow)
    # =========================================================================

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        """
        Calculate the next DAG run info.

        This is the main method Airflow calls to determine scheduling.

        Args:
            last_automated_data_interval: The data interval of the last automated run
            restriction: Time restrictions (earliest/latest allowed)

        Returns:
            DagRunInfo with next run details, or None if no more runs
        """
        # TODO: Implement the main scheduling logic
        # Steps:
        # 1. Determine the starting point (last run or restriction.earliest)
        # 2. Find the next valid run time
        # 3. Check against restriction.latest
        # 4. Return DagRunInfo with data interval

        # Hint: Data interval for point-in-time schedules:
        # - start = run_time
        # - end = run_time
        pass

    def infer_manual_data_interval(
        self,
        *,
        run_after: DateTime,
    ) -> DataInterval:
        """
        Infer data interval for a manual trigger.

        Args:
            run_after: When the manual run was triggered

        Returns:
            DataInterval for the manual run
        """
        # TODO: Implement manual trigger handling
        # For point-in-time schedules, typically use run_after as both start and end
        pass

    # =========================================================================
    # UI DISPLAY (Optional but recommended)
    # =========================================================================

    @property
    def summary(self) -> str:
        """Human-readable summary for Airflow UI."""
        return f"Business days @ 9AM (skip 1st Mon, +5PM last Fri) [{self.timezone}]"


# =============================================================================
# TEST DAG
# =============================================================================


def create_test_dag():
    """
    Create a test DAG using the custom timetable.

    This DAG demonstrates the timetable integration.
    """
    from airflow.sdk import dag, task
    from datetime import datetime

    @dag(
        dag_id="exercise_10_2_custom_timetable",
        # TODO: Set the timetable parameter
        # timetable=BusinessDayTimetable(timezone="America/New_York"),
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["exercise", "timetable"],
    )
    def business_day_dag():
        """DAG that runs on business days with custom logic."""

        @task
        def process_business_data(**context):
            """Process data for the business day."""
            logical_date = context["logical_date"]
            print(f"Processing business data for: {logical_date}")

            # Check what kind of run this is
            hour = logical_date.hour
            if hour == 17:
                print("This is a month-end evening run!")
            else:
                print("This is a regular morning run.")

            return {"processed_date": str(logical_date)}

        process_business_data()

    return business_day_dag()


# =============================================================================
# TESTING
# =============================================================================


def test_timetable():
    """Test the timetable logic."""
    print("Testing BusinessDayTimetable...")

    tt = BusinessDayTimetable(timezone="America/New_York")

    # Test helper functions
    print("\n--- Helper Function Tests ---")

    # Test first Monday detection
    jan_2024_first_monday = get_first_monday(2024, 1)
    print(f"First Monday of Jan 2024: {jan_2024_first_monday}")
    # Expected: 2024-01-01 (Jan 1, 2024 is a Monday)

    # Test last Friday detection
    jan_2024_last_friday = get_last_friday(2024, 1)
    print(f"Last Friday of Jan 2024: {jan_2024_last_friday}")
    # Expected: 2024-01-26

    print("\n--- Skip Logic Tests ---")

    # Test dates
    test_dates = [
        pendulum.datetime(2024, 1, 1, 9, 0, tz="America/New_York"),   # First Monday - SKIP
        pendulum.datetime(2024, 1, 2, 9, 0, tz="America/New_York"),   # Tuesday - RUN
        pendulum.datetime(2024, 1, 6, 9, 0, tz="America/New_York"),   # Saturday - SKIP
        pendulum.datetime(2024, 1, 26, 9, 0, tz="America/New_York"),  # Last Friday - RUN
        pendulum.datetime(2024, 1, 26, 17, 0, tz="America/New_York"), # Last Friday 5PM - RUN
    ]

    for dt in test_dates:
        # TODO: Test your skip logic
        # should_skip = tt._should_skip(dt)
        # is_last_fri = tt._is_last_friday(dt)
        # print(f"{dt}: skip={should_skip}, last_friday={is_last_fri}")
        pass

    print("\n--- Next Run Tests ---")

    # Test next run calculation
    start_points = [
        pendulum.datetime(2024, 1, 1, 10, 0, tz="America/New_York"),  # After first Monday morning
        pendulum.datetime(2024, 1, 5, 10, 0, tz="America/New_York"),  # Friday after run
        pendulum.datetime(2024, 1, 25, 10, 0, tz="America/New_York"), # Day before last Friday
    ]

    for start in start_points:
        # TODO: Test next run calculation
        # next_run = tt._get_next_run_time(start)
        # print(f"After {start}: next run = {next_run}")
        pass

    print("\nTimetable testing complete!")


if __name__ == "__main__":
    test_timetable()
