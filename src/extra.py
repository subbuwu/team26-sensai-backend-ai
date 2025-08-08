# Add to src/api/db/__init__.py

# Add to src/api/models.py
from api.utils.db import (
    get_new_db_connection,
    execute_db_operation,
    execute_multiple_db_operations,
)
import os
import aiosqlite
from os.path import exists
from api.utils.db import get_new_db_connection, check_table_exists, set_db_defaults
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum

from api.config import (
    cohorts_table_name,
    course_cohorts_table_name,
    courses_table_name,
    tasks_table_name,
    chat_history_table_name,
    user_cohorts_table_name,
    organizations_table_name,
    user_organizations_table_name,
    users_table_name,
    questions_table_name
)
# Assessment submissions table - tracks formal quiz/exam submissions
assessment_submissions_table_name = "assessment_submissions"
assessment_submissions_table = f"""
CREATE TABLE IF NOT EXISTS {assessment_submissions_table_name} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    task_id INTEGER NOT NULL,
    cohort_id INTEGER,
    course_id INTEGER,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    submitted_at TIMESTAMP,
    time_spent_seconds INTEGER DEFAULT 0,
    total_score REAL DEFAULT 0.0,
    max_possible_score REAL DEFAULT 0.0,
    percentage_score REAL DEFAULT 0.0,
    status TEXT DEFAULT 'in_progress', -- 'in_progress', 'submitted', 'graded'
    attempt_number INTEGER DEFAULT 1,
    is_final_submission BOOLEAN DEFAULT FALSE,
    metadata TEXT, -- JSON for additional data
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES {users_table_name}(id),
    FOREIGN KEY (task_id) REFERENCES {tasks_table_name}(id),
    FOREIGN KEY (cohort_id) REFERENCES {cohorts_table_name}(id),
    FOREIGN KEY (course_id) REFERENCES {courses_table_name}(id)
)
"""

# Question responses table - detailed responses for each question in a submission
question_responses_table_name = "question_responses"
question_responses_table = f"""
CREATE TABLE IF NOT EXISTS {question_responses_table_name} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    submission_id INTEGER NOT NULL,
    question_id INTEGER NOT NULL,
    user_response TEXT, -- JSON or text response
    user_response_type TEXT DEFAULT 'text', -- 'text', 'code', 'audio'
    ai_feedback TEXT, -- AI-generated feedback
    score REAL DEFAULT 0.0,
    max_score REAL DEFAULT 0.0,
    is_correct BOOLEAN,
    time_spent_seconds INTEGER DEFAULT 0,
    attempt_count INTEGER DEFAULT 1,
    submitted_at TIMESTAMP,
    graded_at TIMESTAMP,
    graded_by TEXT DEFAULT 'ai', -- 'ai', 'instructor', 'peer'
    scorecard_results TEXT, -- JSON for detailed scorecard breakdown
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (submission_id) REFERENCES {assessment_submissions_table_name}(id),
    FOREIGN KEY (question_id) REFERENCES {questions_table_name}(id)
)
"""

# Leaderboard entries table - for ranking and competition features
leaderboard_entries_table_name = "leaderboard_entries"
leaderboard_entries_table = f"""
CREATE TABLE IF NOT EXISTS {leaderboard_entries_table_name} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    cohort_id INTEGER,
    course_id INTEGER,
    task_id INTEGER,
    score REAL NOT NULL,
    max_score REAL NOT NULL,
    percentage REAL NOT NULL,
    rank_position INTEGER,
    submission_id INTEGER,
    achievement_badges TEXT, -- JSON array of earned badges
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES {users_table_name}(id),
    FOREIGN KEY (cohort_id) REFERENCES {cohorts_table_name}(id),
    FOREIGN KEY (course_id) REFERENCES {courses_table_name}(id),
    FOREIGN KEY (task_id) REFERENCES {tasks_table_name}(id),
    FOREIGN KEY (submission_id) REFERENCES {assessment_submissions_table_name}(id)
)
"""

# Assessment analytics table - aggregated metrics for instructors
assessment_analytics_table_name = "assessment_analytics"
assessment_analytics_table = f"""
CREATE TABLE IF NOT EXISTS {assessment_analytics_table_name} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL,
    cohort_id INTEGER,
    course_id INTEGER,
    total_submissions INTEGER DEFAULT 0,
    avg_score REAL DEFAULT 0.0,
    median_score REAL DEFAULT 0.0,
    highest_score REAL DEFAULT 0.0,
    lowest_score REAL DEFAULT 0.0,
    avg_time_minutes REAL DEFAULT 0.0,
    completion_rate REAL DEFAULT 0.0,
    difficulty_rating REAL DEFAULT 0.0, -- calculated based on performance
    question_analytics TEXT, -- JSON with per-question statistics
    last_calculated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (task_id) REFERENCES {tasks_table_name}(id),
    FOREIGN KEY (cohort_id) REFERENCES {cohorts_table_name}(id),
    FOREIGN KEY (course_id) REFERENCES {courses_table_name}(id)
)
"""

# Add indexes for performance
assessment_indexes = [
    f"CREATE INDEX IF NOT EXISTS idx_submissions_user_task ON {assessment_submissions_table_name}(user_id, task_id)",
    f"CREATE INDEX IF NOT EXISTS idx_submissions_cohort ON {assessment_submissions_table_name}(cohort_id)",
    f"CREATE INDEX IF NOT EXISTS idx_submissions_status ON {assessment_submissions_table_name}(status)",
    f"CREATE INDEX IF NOT EXISTS idx_responses_submission ON {question_responses_table_name}(submission_id)",
    f"CREATE INDEX IF NOT EXISTS idx_responses_question ON {question_responses_table_name}(question_id)",
    f"CREATE INDEX IF NOT EXISTS idx_leaderboard_cohort ON {leaderboard_entries_table_name}(cohort_id, percentage DESC)",
    f"CREATE INDEX IF NOT EXISTS idx_analytics_task ON {assessment_analytics_table_name}(task_id)"
]

# Add to database initialization function
async def initialize_assessment_tables():
    """Initialize assessment-related database tables"""
    await execute_db_operation(assessment_submissions_table)
    await execute_db_operation(question_responses_table)
    await execute_db_operation(leaderboard_entries_table)
    await execute_db_operation(assessment_analytics_table)
    
    # Create indexes
    for index_sql in assessment_indexes:
        await execute_db_operation(index_sql)
    
    print("Assessment database tables initialized successfully")

# Add table names to exports
__all__ = [
    "assessment_submissions_table_name",
    "question_responses_table_name", 
    "leaderboard_entries_table_name",
    "assessment_analytics_table_name",
    "initialize_assessment_tables"
]

import asyncio

if __name__ == "__main__":
    asyncio.run(initialize_assessment_tables())