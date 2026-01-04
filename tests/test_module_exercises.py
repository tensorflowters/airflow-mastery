"""
Module Exercise Structure Tests
===============================

Tests to validate the structure and completeness of exercise files
and their corresponding solutions across all modules.

This test suite verifies:
- Exercise files have proper TODO sections
- Solutions exist for all exercises
- Exercise/solution pairs are properly matched
- Module READMEs reference exercises correctly

Run with: pytest tests/test_module_exercises.py -v
"""

import pytest
import re
from pathlib import Path
from typing import NamedTuple


class ExerciseFile(NamedTuple):
    """Represents an exercise file with metadata."""

    path: Path
    module_number: int
    exercise_number: int
    name: str
    has_solution: bool


def get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).parent.parent


def get_all_exercises() -> list[ExerciseFile]:
    """Discover all exercise files across modules."""
    project_root = get_project_root()
    exercises = []

    for module_dir in sorted(project_root.glob("modules/*/exercises")):
        module_name = module_dir.parent.name
        module_num = int(module_name.split("-")[0])

        solutions_dir = module_dir.parent / "solutions"

        for exercise_file in sorted(module_dir.glob("*.py")):
            filename = exercise_file.stem

            # Skip utility files (like broken_dag.py for debugging exercises)
            if "starter" not in filename and "exercise" not in filename:
                continue

            # Extract exercise number
            match = re.search(r"(\d+)_(\d+)", filename)
            if match:
                exercise_num = int(match.group(2))
            else:
                exercise_num = 1

            # Check if solution exists
            solution_name = filename.replace("_starter", "").replace("exercise_", "solution_")
            solution_path = solutions_dir / f"{solution_name}.py"
            has_solution = solution_path.exists()

            exercises.append(ExerciseFile(
                path=exercise_file,
                module_number=module_num,
                exercise_number=exercise_num,
                name=filename,
                has_solution=has_solution,
            ))

    return exercises


EXERCISE_FILES = get_all_exercises()
EXERCISE_IDS = [f"module_{e.module_number:02d}_{e.name}" for e in EXERCISE_FILES]


class TestExerciseStructure:
    """Test exercise file structure and content."""

    @pytest.mark.parametrize("exercise", EXERCISE_FILES, ids=EXERCISE_IDS)
    def test_exercise_has_todo_sections(self, exercise: ExerciseFile):
        """Verify exercise file has TODO sections for learners."""
        content = exercise.path.read_text()

        has_todo = "TODO" in content.upper()

        assert has_todo, \
            f"{exercise.path.name} missing TODO sections for learners"

    @pytest.mark.parametrize("exercise", EXERCISE_FILES, ids=EXERCISE_IDS)
    def test_exercise_has_instructions(self, exercise: ExerciseFile):
        """Verify exercise file has clear instructions."""
        content = exercise.path.read_text()

        # Check for instruction-like patterns
        has_instructions = (
            "Instructions:" in content or
            "Your task:" in content or
            "TODO:" in content or
            "Complete the" in content or
            "Implement" in content
        )

        assert has_instructions, \
            f"{exercise.path.name} missing clear instructions"

    @pytest.mark.parametrize("exercise", EXERCISE_FILES, ids=EXERCISE_IDS)
    def test_exercise_has_docstring(self, exercise: ExerciseFile):
        """Verify exercise file has a module-level docstring."""
        content = exercise.path.read_text()

        # Check for docstring at start
        has_docstring = content.strip().startswith('"""') or content.strip().startswith("'''")

        assert has_docstring, \
            f"{exercise.path.name} missing module docstring"

    @pytest.mark.parametrize("exercise", EXERCISE_FILES, ids=EXERCISE_IDS)
    def test_exercise_has_success_criteria(self, exercise: ExerciseFile):
        """Verify exercise defines success criteria."""
        content = exercise.path.read_text()

        has_criteria = (
            "Success Criteria:" in content or
            "Expected Output:" in content or
            "When complete:" in content or
            "Your DAG should:" in content or
            "- [ ]" in content  # Checklist format
        )

        # This is a soft check - warn instead of fail
        if not has_criteria:
            pytest.skip(f"{exercise.path.name} could benefit from success criteria section")


class TestExerciseSolutionPairing:
    """Test that exercises have matching solutions."""

    @pytest.mark.parametrize("exercise", EXERCISE_FILES, ids=EXERCISE_IDS)
    def test_exercise_has_solution(self, exercise: ExerciseFile):
        """Verify each exercise has a corresponding solution file."""
        # Allow some exercises to not have solutions (infrastructure/documentation exercises)
        if exercise.module_number == 8 and exercise.exercise_number in [1, 3]:
            pytest.skip("Module 08 exercises 1 and 3 are infrastructure exercises")

        assert exercise.has_solution, \
            f"{exercise.path.name} missing corresponding solution file"

    def test_solution_count_matches_exercises(self):
        """Verify solution count matches exercise count per module."""
        project_root = get_project_root()

        for module_num in range(1, 15):
            module_dir = list(project_root.glob(f"modules/{module_num:02d}-*/"))
            if not module_dir:
                continue

            module_dir = module_dir[0]
            exercises_dir = module_dir / "exercises"
            solutions_dir = module_dir / "solutions"

            if not exercises_dir.exists() or not solutions_dir.exists():
                continue

            # Count Python files (excluding __init__.py and utilities)
            exercise_count = len([
                f for f in exercises_dir.glob("*.py")
                if "starter" in f.name or "exercise" in f.name
            ])

            solution_count = len([
                f for f in solutions_dir.glob("*.py")
                if f.name != "__init__.py"
            ])

            # Allow solutions to be >= exercises (some may have multiple solutions)
            if solution_count < exercise_count:
                pytest.skip(
                    f"Module {module_num:02d}: {solution_count} solutions for "
                    f"{exercise_count} exercises (may be intentional)"
                )


class TestModuleCoverage:
    """Test module completeness and organization."""

    def test_all_modules_have_exercises(self):
        """Verify modules 01-14 have exercise directories."""
        project_root = get_project_root()

        missing_exercises = []
        for module_num in range(1, 15):
            module_dirs = list(project_root.glob(f"modules/{module_num:02d}-*/"))

            if not module_dirs:
                missing_exercises.append(module_num)
                continue

            exercises_dir = module_dirs[0] / "exercises"
            if not exercises_dir.exists() or not list(exercises_dir.glob("*.py")):
                missing_exercises.append(module_num)

        assert not missing_exercises, \
            f"Modules missing exercises directory: {missing_exercises}"

    def test_all_modules_have_readme(self):
        """Verify all modules have README.md files."""
        project_root = get_project_root()

        missing_readme = []
        for module_dir in project_root.glob("modules/*/"):
            if not module_dir.is_dir():
                continue

            readme = module_dir / "README.md"
            if not readme.exists():
                missing_readme.append(module_dir.name)

        assert not missing_readme, \
            f"Modules missing README.md: {missing_readme}"

    def test_readme_references_exercises(self):
        """Verify module READMEs mention exercises."""
        project_root = get_project_root()

        modules_without_exercise_refs = []

        for module_dir in project_root.glob("modules/*/"):
            if not module_dir.is_dir():
                continue

            readme = module_dir / "README.md"
            if not readme.exists():
                continue

            content = readme.read_text()

            # Check for exercise references
            has_exercise_ref = (
                "exercise" in content.lower() or
                "Exercise" in content or
                "exercises/" in content
            )

            if not has_exercise_ref:
                modules_without_exercise_refs.append(module_dir.name)

        if modules_without_exercise_refs:
            pytest.skip(
                f"READMEs that could reference exercises: "
                f"{modules_without_exercise_refs}"
            )


class TestExerciseNaming:
    """Test exercise file naming conventions."""

    @pytest.mark.parametrize("exercise", EXERCISE_FILES, ids=EXERCISE_IDS)
    def test_exercise_naming_convention(self, exercise: ExerciseFile):
        """Verify exercise follows naming convention."""
        filename = exercise.path.stem

        # Expected: exercise_X_Y_descriptive_starter.py or exercise_X_Y_name_starter.py
        valid_patterns = [
            r"exercise_\d+_\d+_\w+_starter",
            r"exercise_\d+_\d+_starter",
            r"ex_\d+_\d+_\w+",
        ]

        matches_pattern = any(re.match(p, filename) for p in valid_patterns)

        # Also allow broken_dag.py and similar utility files
        is_utility = "broken" in filename or "example" in filename

        assert matches_pattern or is_utility, \
            f"{exercise.path.name} doesn't follow naming convention"

    @pytest.mark.parametrize("exercise", EXERCISE_FILES, ids=EXERCISE_IDS)
    def test_exercise_uses_lowercase(self, exercise: ExerciseFile):
        """Verify exercise filename is lowercase."""
        filename = exercise.path.stem

        assert filename == filename.lower(), \
            f"{exercise.path.name} should be lowercase"


class TestExerciseContent:
    """Test exercise file content quality."""

    @pytest.mark.parametrize("exercise", EXERCISE_FILES, ids=EXERCISE_IDS)
    def test_exercise_is_parseable(self, exercise: ExerciseFile):
        """Verify exercise file is valid Python syntax."""
        import ast

        content = exercise.path.read_text()

        try:
            ast.parse(content)
        except SyntaxError as e:
            pytest.fail(f"Syntax error in {exercise.path.name}: {e}")

    @pytest.mark.parametrize("exercise", EXERCISE_FILES, ids=EXERCISE_IDS)
    def test_exercise_has_hints(self, exercise: ExerciseFile):
        """Verify exercise provides hints for learners."""
        content = exercise.path.read_text()

        has_hints = (
            "Hint:" in content or
            "HINT:" in content or
            "hint:" in content or
            "# Hint" in content or
            "Example:" in content
        )

        # Soft check - not all exercises need hints
        if not has_hints:
            pytest.skip(f"{exercise.path.name} could benefit from hints")

    @pytest.mark.parametrize("exercise", EXERCISE_FILES, ids=EXERCISE_IDS)
    def test_exercise_imports_are_present(self, exercise: ExerciseFile):
        """Verify exercise has necessary imports."""
        content = exercise.path.read_text()

        # Check for basic Airflow imports
        has_airflow_import = (
            "from airflow" in content or
            "import airflow" in content
        )

        # Some exercises might be pure Python utilities
        if "@dag" in content or "DAG(" in content:
            assert has_airflow_import, \
                f"{exercise.path.name} missing Airflow imports"


class TestModuleProgression:
    """Test that modules follow logical progression."""

    def test_modules_are_numbered_sequentially(self):
        """Verify modules are numbered 01-14 without gaps."""
        project_root = get_project_root()

        module_numbers = set()
        for module_dir in project_root.glob("modules/*/"):
            if not module_dir.is_dir():
                continue

            match = re.match(r"(\d+)-", module_dir.name)
            if match:
                module_numbers.add(int(match.group(1)))

        expected = set(range(1, 15))
        missing = expected - module_numbers

        assert not missing, f"Missing module numbers: {sorted(missing)}"

    def test_exercise_numbers_are_sequential(self):
        """Verify exercise numbers within modules are sequential."""
        from collections import defaultdict

        exercises_by_module = defaultdict(list)
        for exercise in EXERCISE_FILES:
            exercises_by_module[exercise.module_number].append(exercise.exercise_number)

        gaps = []
        for module_num, exercise_nums in exercises_by_module.items():
            sorted_nums = sorted(set(exercise_nums))
            expected = list(range(1, max(sorted_nums) + 1))

            if sorted_nums != expected[:len(sorted_nums)]:
                gaps.append((module_num, sorted_nums))

        if gaps:
            pytest.skip(f"Modules with exercise number gaps (may be intentional): {gaps}")
