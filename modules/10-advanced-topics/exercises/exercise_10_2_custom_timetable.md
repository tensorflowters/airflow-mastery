# Exercise 10.2: Custom Timetable

## Objective

Create a custom timetable that implements complex business scheduling logic beyond what cron expressions can handle.

## Background

Airflow 3's timetable system allows custom scheduling logic for scenarios like:
- Business day calculations
- Holiday-aware scheduling
- Variable intervals based on data conditions
- Month-relative scheduling (first Monday, last Friday)

### Timetable Architecture

```python
from airflow.timetables.base import DagRunInfo, DataInterval, Timetable
from pendulum import DateTime

class CustomTimetable(Timetable):
    """Custom scheduling logic."""

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        """Calculate next DAG run timing."""
        pass

    def infer_manual_data_interval(
        self,
        *,
        run_after: DateTime,
    ) -> DataInterval:
        """Handle manual triggers."""
        pass
```

## Requirements

Create a timetable implementing this business schedule:

### Part 1: Basic Weekday Schedule
1. Run every weekday (Monday-Friday) at 9:00 AM
2. Skip weekends entirely
3. Handle timezone correctly (use business timezone)

### Part 2: Month-Relative Exceptions
1. **Skip** the first Monday of each month (monthly planning meetings)
2. Add an **extra run** on the last Friday of each month at 5:00 PM (month-end processing)

### Part 3: Holiday Awareness (Bonus)
1. Define a list of company holidays
2. Skip runs on holidays
3. Optionally reschedule to next business day

## Expected Schedule Examples

```
January 2024:
- Mon Jan 1  - SKIP (holiday: New Year)
- Tue Jan 2  - RUN 9:00 AM
- Wed Jan 3  - RUN 9:00 AM
- ...
- Mon Jan 8  - SKIP (first Monday of month)
- Tue Jan 9  - RUN 9:00 AM
- ...
- Fri Jan 26 - RUN 9:00 AM AND 5:00 PM (last Friday)
- Mon Jan 29 - RUN 9:00 AM
- ...
```

## Starter Code

See `exercise_10_2_custom_timetable_starter.py`

## Hints

<details>
<summary>Hint 1: Finding month-relative days</summary>

```python
import pendulum
from pendulum import DateTime

def get_first_monday(year: int, month: int) -> DateTime:
    """Get the first Monday of a month."""
    first_day = pendulum.datetime(year, month, 1)
    # Monday is weekday 0
    days_until_monday = (7 - first_day.weekday()) % 7
    if first_day.weekday() == 0:  # Already Monday
        return first_day
    return first_day.add(days=days_until_monday)

def get_last_friday(year: int, month: int) -> DateTime:
    """Get the last Friday of a month."""
    # Go to last day of month
    last_day = pendulum.datetime(year, month, 1).end_of("month")
    # Friday is weekday 4
    days_since_friday = (last_day.weekday() - 4) % 7
    return last_day.subtract(days=days_since_friday)
```

</details>

<details>
<summary>Hint 2: Timetable serialization</summary>

```python
class BusinessDayTimetable(Timetable):
    """Timetables must be serializable for the database."""

    def __init__(self, timezone: str = "America/New_York"):
        self.timezone = timezone

    def serialize(self) -> dict:
        """Serialize for storage."""
        return {"timezone": self.timezone}

    @classmethod
    def deserialize(cls, data: dict) -> "BusinessDayTimetable":
        """Deserialize from storage."""
        return cls(timezone=data["timezone"])
```

</details>

<details>
<summary>Hint 3: Next run calculation</summary>

```python
def _get_next_run(self, after: DateTime) -> DateTime | None:
    """Calculate next valid run time."""
    candidate = after.add(days=1).set(hour=9, minute=0, second=0)

    # Skip to Monday if weekend
    if candidate.weekday() == 5:  # Saturday
        candidate = candidate.add(days=2)
    elif candidate.weekday() == 6:  # Sunday
        candidate = candidate.add(days=1)

    # Check for first Monday skip
    first_monday = self._get_first_monday(candidate.year, candidate.month)
    if candidate.date() == first_monday.date():
        candidate = candidate.add(days=1)

    return candidate
```

</details>

## Verification

Test your timetable:

```python
from pendulum import datetime
from your_timetable import BusinessDayTimetable

tt = BusinessDayTimetable(timezone="America/New_York")

# Test next run calculation
test_date = datetime(2024, 1, 5, 10, 0, 0)  # Friday after run
next_run = tt._get_next_run(test_date)
print(f"Next run after {test_date}: {next_run}")
# Should be Monday Jan 8 (skip weekend) but wait - that's first Monday!
# Should actually be Tuesday Jan 9

# Test last Friday detection
assert tt._is_last_friday(datetime(2024, 1, 26)) == True
assert tt._is_last_friday(datetime(2024, 1, 19)) == False
```

## Success Criteria

- [ ] Runs every weekday at 9:00 AM
- [ ] Correctly skips weekends
- [ ] Skips first Monday of each month
- [ ] Adds extra 5:00 PM run on last Friday
- [ ] Handles timezone correctly
- [ ] Properly serializes/deserializes
- [ ] Works with manual triggers
- [ ] Integrates with a test DAG

## Bonus Challenges

1. **Holiday Calendar Integration**: Use `holidays` library for automatic holiday detection
2. **Configurable Skip Days**: Make the skip rules configurable per-DAG
3. **Catchup Behavior**: Handle catchup runs correctly for historical data intervals
4. **UI Display**: Implement `summary` property for nice display in Airflow UI

---

Next: [Exercise 10.3: Plugin Development →](exercise_10_3_plugin_development.md)
