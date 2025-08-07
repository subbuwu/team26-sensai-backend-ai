import os
import aiosqlite
from os.path import exists
from api.utils.db import get_new_db_connection, check_table_exists, set_db_defaults
from api.config import (
    sqlite_db_path,
    chat_history_table_name,
    tasks_table_name,
    questions_table_name,
    cohorts_table_name,
    user_cohorts_table_name,
    milestones_table_name,
    users_table_name,
    organizations_table_name,
    user_organizations_table_name,
    courses_table_name,
    course_cohorts_table_name,
    course_tasks_table_name,
    uncategorized_milestone_name,
    course_milestones_table_name,
    group_role_learner,
    group_role_mentor,
    uncategorized_milestone_color,
    task_completions_table_name,
    scorecards_table_name,
    question_scorecards_table_name,
    course_generation_jobs_table_name,
    task_generation_jobs_table_name,
    org_api_keys_table_name,
    code_drafts_table_name,
)
import logging

# Set up logging
logger = logging.getLogger(__name__)


async def create_organizations_table(cursor):
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {organizations_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slug TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                default_logo_color TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                openai_api_key TEXT,
                openai_free_trial BOOLEAN
            )"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_org_slug ON {organizations_table_name} (slug)"""
    )


async def create_org_api_keys_table(cursor):
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {org_api_keys_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id INTEGER NOT NULL,
                hashed_key TEXT NOT NULL UNIQUE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (org_id) REFERENCES {organizations_table_name}(id) ON DELETE CASCADE
            )"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_org_api_key_org_id ON {org_api_keys_table_name} (org_id)"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_org_api_key_hashed_key ON {org_api_keys_table_name} (hashed_key)"""
    )


async def create_users_table(cursor):
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {users_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                first_name TEXT,
                middle_name TEXT,
                last_name TEXT,
                default_dp_color TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )"""
    )


async def create_user_organizations_table(cursor):
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {user_organizations_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                org_id INTEGER NOT NULL,
                role TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, org_id),
                FOREIGN KEY (user_id) REFERENCES {users_table_name}(id) ON DELETE CASCADE,
                FOREIGN KEY (org_id) REFERENCES {organizations_table_name}(id) ON DELETE CASCADE
            )"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_user_org_user_id ON {user_organizations_table_name} (user_id)"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_user_org_org_id ON {user_organizations_table_name} (org_id)"""
    )


async def create_cohort_tables(cursor):
    # Create a table to store cohorts
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {cohorts_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                org_id INTEGER NOT NULL,
                FOREIGN KEY (org_id) REFERENCES {organizations_table_name}(id) ON DELETE CASCADE
            )"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_cohort_org_id ON {cohorts_table_name} (org_id)"""
    )

    # Create a table to store users in cohorts
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {user_cohorts_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                cohort_id INTEGER NOT NULL,
                role TEXT NOT NULL,
                joined_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, cohort_id),
                FOREIGN KEY (user_id) REFERENCES {users_table_name}(id) ON DELETE CASCADE,
                FOREIGN KEY (cohort_id) REFERENCES {cohorts_table_name}(id) ON DELETE CASCADE
            )"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_user_cohort_user_id ON {user_cohorts_table_name} (user_id)"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_user_cohort_cohort_id ON {user_cohorts_table_name} (cohort_id)"""
    )


async def create_courses_table(cursor):
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {courses_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (org_id) REFERENCES {organizations_table_name}(id) ON DELETE CASCADE
            )"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_course_org_id ON {courses_table_name} (org_id)"""
    )


async def create_course_cohorts_table(cursor):
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {course_cohorts_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                cohort_id INTEGER NOT NULL,
                is_drip_enabled BOOLEAN DEFAULT FALSE,
                frequency_value INTEGER,
                frequency_unit TEXT,
                publish_at DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(course_id, cohort_id),
                FOREIGN KEY (course_id) REFERENCES {courses_table_name}(id) ON DELETE CASCADE,
                FOREIGN KEY (cohort_id) REFERENCES {cohorts_table_name}(id) ON DELETE CASCADE
            )"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_course_cohort_course_id ON {course_cohorts_table_name} (course_id)"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_course_cohort_cohort_id ON {course_cohorts_table_name} (cohort_id)"""
    )


async def create_milestones_table(cursor):
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {milestones_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                color TEXT,
                FOREIGN KEY (org_id) REFERENCES {organizations_table_name}(id) ON DELETE CASCADE
            )"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_milestone_org_id ON {milestones_table_name} (org_id)"""
    )


async def create_course_milestones_table(cursor):
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {course_milestones_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course_id INTEGER NOT NULL,
                milestone_id INTEGER,
                ordering INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(course_id, milestone_id),
                FOREIGN KEY (course_id) REFERENCES {courses_table_name}(id) ON DELETE CASCADE,
                FOREIGN KEY (milestone_id) REFERENCES {milestones_table_name}(id) ON DELETE CASCADE
            )"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_course_milestone_course_id ON {course_milestones_table_name} (course_id)"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_course_milestone_milestone_id ON {course_milestones_table_name} (milestone_id)"""
    )


async def create_tasks_table(cursor):
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {tasks_table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    org_id INTEGER NOT NULL,
                    type TEXT NOT NULL,
                    blocks TEXT,
                    title TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    deleted_at DATETIME,
                    scheduled_publish_at DATETIME,
                    FOREIGN KEY (org_id) REFERENCES {organizations_table_name}(id) ON DELETE CASCADE
                )"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_task_org_id ON {tasks_table_name} (org_id)"""
    )


async def create_course_tasks_table(cursor):
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {course_tasks_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                course_id INTEGER NOT NULL,
                ordering INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                milestone_id INTEGER,
                UNIQUE(task_id, course_id),
                FOREIGN KEY (task_id) REFERENCES {tasks_table_name}(id) ON DELETE CASCADE,
                FOREIGN KEY (course_id) REFERENCES {courses_table_name}(id) ON DELETE CASCADE,
                FOREIGN KEY (milestone_id) REFERENCES {milestones_table_name}(id) ON DELETE CASCADE
            )"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_course_task_task_id ON {course_tasks_table_name} (task_id)"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_course_task_course_id ON {course_tasks_table_name} (course_id)"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_course_task_milestone_id ON {course_tasks_table_name} (milestone_id)"""
    )


async def create_questions_table(cursor):
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {questions_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                blocks TEXT,
                answer TEXT,
                input_type TEXT NOT NULL,
                coding_language TEXT,
                generation_model TEXT,
                response_type TEXT NOT NULL,
                position INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                deleted_at DATETIME,
                max_attempts INTEGER,
                is_feedback_shown BOOLEAN NOT NULL,
                context TEXT,
                title TEXT NOT NULL,
                FOREIGN KEY (task_id) REFERENCES {tasks_table_name}(id) ON DELETE CASCADE
            )"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_question_task_id ON {questions_table_name} (task_id)"""
    )


async def create_scorecards_table(cursor):
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {scorecards_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                org_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                criteria TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                status TEXT,
                FOREIGN KEY (org_id) REFERENCES {organizations_table_name}(id) ON DELETE CASCADE
            )"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_scorecard_org_id ON {scorecards_table_name} (org_id)"""
    )


async def create_question_scorecards_table(cursor):
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {question_scorecards_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                question_id INTEGER NOT NULL,
                scorecard_id INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (question_id) REFERENCES {questions_table_name}(id) ON DELETE CASCADE,
                FOREIGN KEY (scorecard_id) REFERENCES {scorecards_table_name}(id) ON DELETE CASCADE,
                UNIQUE(question_id, scorecard_id)
            )"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_question_scorecard_question_id ON {question_scorecards_table_name} (question_id)"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_question_scorecard_scorecard_id ON {question_scorecards_table_name} (scorecard_id)"""
    )


async def create_chat_history_table(cursor):
    await cursor.execute(
        f"""
                CREATE TABLE IF NOT EXISTS {chat_history_table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    question_id INTEGER NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT,
                    response_type TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (question_id) REFERENCES {questions_table_name}(id),
                    FOREIGN KEY (user_id) REFERENCES {users_table_name}(id) ON DELETE CASCADE
                )"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_chat_history_user_id ON {chat_history_table_name} (user_id)"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_chat_history_question_id ON {chat_history_table_name} (question_id)"""
    )


async def create_task_completion_table(cursor):
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {task_completions_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                task_id INTEGER,
                question_id INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES {users_table_name}(id) ON DELETE CASCADE,
                FOREIGN KEY (task_id) REFERENCES {tasks_table_name}(id) ON DELETE CASCADE,
                FOREIGN KEY (question_id) REFERENCES {questions_table_name}(id) ON DELETE CASCADE,
                UNIQUE(user_id, task_id),
                UNIQUE(user_id, question_id)
            )"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_task_completion_user_id ON {task_completions_table_name} (user_id)"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_task_completion_task_id ON {task_completions_table_name} (task_id)"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_task_completion_question_id ON {task_completions_table_name} (question_id)"""
    )


async def create_course_generation_jobs_table(cursor):
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {course_generation_jobs_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid TEXT NOT NULL,
                course_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                job_details TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (course_id) REFERENCES {courses_table_name}(id) ON DELETE CASCADE
            )"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_course_generation_job_course_id ON {course_generation_jobs_table_name} (course_id)"""
    )


async def create_task_generation_jobs_table(cursor):
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {task_generation_jobs_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid TEXT NOT NULL,
                task_id INTEGER NOT NULL,
                course_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                job_details TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (task_id) REFERENCES {tasks_table_name}(id) ON DELETE CASCADE,
                FOREIGN KEY (course_id) REFERENCES {courses_table_name}(id) ON DELETE CASCADE
            )"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_task_generation_job_task_id ON {task_generation_jobs_table_name} (task_id)"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_task_generation_job_course_id ON {task_generation_jobs_table_name} (course_id)"""
    )


async def create_code_drafts_table(cursor):
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {code_drafts_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                question_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, question_id),
                FOREIGN KEY (user_id) REFERENCES {users_table_name}(id) ON DELETE CASCADE,
                FOREIGN KEY (question_id) REFERENCES {questions_table_name}(id) ON DELETE CASCADE
            )"""
    )

    # Useful indexes for faster lookup
    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_code_drafts_user_id ON {code_drafts_table_name} (user_id)"""
    )

    await cursor.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_code_drafts_question_id ON {code_drafts_table_name} (question_id)"""
    )


async def init_db():
    """Initialize the database with improved error handling and WAL mode"""
    
    # Ensure the database folder exists
    db_folder = os.path.dirname(sqlite_db_path)
    if not os.path.exists(db_folder):
        os.makedirs(db_folder)
        logger.info(f"Created database folder: {db_folder}")

    # Check if database exists for first-time setup
    is_new_db = not exists(sqlite_db_path)
    
    if is_new_db:
        logger.info(f"Creating new database at: {sqlite_db_path}")
        # Only set defaults for new database
        set_db_defaults()
    else:
        logger.info(f"Database already exists at: {sqlite_db_path}")

    try:
        async with get_new_db_connection() as conn:
            cursor = await conn.cursor()
            
            # Set WAL mode for better concurrency (as mentioned in project docs)
            await cursor.execute("PRAGMA journal_mode=WAL")
            await cursor.execute("PRAGMA foreign_keys=ON")
            
            # If database exists, just check for missing tables
            if not is_new_db:
                # Check for code_drafts table (newest addition)
                if not await check_table_exists(code_drafts_table_name, cursor):
                    logger.info("Adding missing code_drafts table")
                    await create_code_drafts_table(cursor)
                
                await conn.commit()
                logger.info("Database migration completed successfully")
                return

            # Create all tables in correct dependency order
            logger.info("Creating database tables...")
            
            # Organizations and users (no dependencies)
            await create_organizations_table(cursor)
            await create_users_table(cursor)
            
            # Tables that depend on organizations/users
            await create_org_api_keys_table(cursor)
            await create_user_organizations_table(cursor)
            await create_milestones_table(cursor)
            await create_cohort_tables(cursor)
            await create_courses_table(cursor)
            
            # Tables that depend on courses/cohorts
            await create_course_cohorts_table(cursor)
            await create_tasks_table(cursor)
            
            # Tables that depend on tasks
            await create_questions_table(cursor)
            await create_scorecards_table(cursor)
            await create_question_scorecards_table(cursor)
            await create_chat_history_table(cursor)
            await create_task_completion_table(cursor)
            
            # Tables that depend on courses and tasks
            await create_course_tasks_table(cursor)
            await create_course_milestones_table(cursor)
            
            # Job tracking tables
            await create_course_generation_jobs_table(cursor)
            await create_task_generation_jobs_table(cursor)
            
            # Draft tables
            await create_code_drafts_table(cursor)

            await conn.commit()
            logger.info("All tables created successfully")

    except Exception as e:
        logger.error(f"Database initialization failed: {str(e)}")
        
        # Only delete database if it's new and creation failed
        if is_new_db and exists(sqlite_db_path):
            try:
                os.remove(sqlite_db_path)
                logger.info("Removed incomplete database file")
            except Exception as del_err:
                logger.error(f"Failed to remove database file: {del_err}")
        
        # Re-raise with more context
        raise Exception(f"Database initialization failed: {str(e)}") from e


async def delete_useless_tables():
    """Clean up deprecated tables and migrate existing ones"""
    from api.config import (
        tags_table_name,
        task_tags_table_name,
        groups_table_name,
        user_groups_table_name,
        badges_table_name,
        task_scoring_criteria_table_name,
        cv_review_usage_table_name,
        tests_table_name,
    )

    try:
        async with get_new_db_connection() as conn:
            cursor = await conn.cursor()
            
            # Set WAL mode
            await cursor.execute("PRAGMA journal_mode=WAL")
            await cursor.execute("PRAGMA foreign_keys=OFF")  # Temporarily disable for migrations

            # Drop deprecated tables
            deprecated_tables = [
                tags_table_name,
                task_tags_table_name,
                tests_table_name,
                groups_table_name,
                user_groups_table_name,
                badges_table_name,
                task_scoring_criteria_table_name,
                cv_review_usage_table_name,
            ]
            
            for table in deprecated_tables:
                await cursor.execute(f"DROP TABLE IF EXISTS {table}")
                logger.info(f"Dropped deprecated table: {table}")

            await conn.commit()

        # Migrate user_cohorts table if needed
        async with get_new_db_connection() as conn:
            cursor = await conn.cursor()
            
            # Re-enable foreign keys
            await cursor.execute("PRAGMA foreign_keys=ON")
            
            # Check if user_cohorts needs migration
            await cursor.execute(f"PRAGMA table_info({user_cohorts_table_name})")
            user_columns = [col[1] for col in await cursor.fetchall()]

            if "joined_at" not in user_columns:
                logger.info("Migrating user_cohorts table to add joined_at column")
                
                await cursor.execute(f"DROP TABLE IF EXISTS {user_cohorts_table_name}_temp")
                await cursor.execute(
                    f"""
                    CREATE TABLE {user_cohorts_table_name}_temp (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        cohort_id INTEGER NOT NULL,
                        role TEXT NOT NULL,
                        joined_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(user_id, cohort_id),
                        FOREIGN KEY (user_id) REFERENCES {users_table_name}(id) ON DELETE CASCADE,
                        FOREIGN KEY (cohort_id) REFERENCES {cohorts_table_name}(id) ON DELETE CASCADE
                    )
                """
                )
                
                await cursor.execute(
                    f"INSERT INTO {user_cohorts_table_name}_temp (id, user_id, cohort_id, role) "
                    f"SELECT id, user_id, cohort_id, role FROM {user_cohorts_table_name}"
                )
                
                await cursor.execute(f"DROP TABLE {user_cohorts_table_name}")
                await cursor.execute(
                    f"ALTER TABLE {user_cohorts_table_name}_temp RENAME TO {user_cohorts_table_name}"
                )

                # Recreate indexes
                await cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_user_cohort_user_id ON {user_cohorts_table_name} (user_id)"
                )
                await cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_user_cohort_cohort_id ON {user_cohorts_table_name} (cohort_id)"
                )

            # Check and add columns to course_cohorts
            await cursor.execute(f"PRAGMA table_info({course_cohorts_table_name})")
            course_columns = [col[1] for col in await cursor.fetchall()]

            columns_to_add = [
                ("is_drip_enabled", "BOOLEAN", "FALSE"),
                ("frequency_value", "INTEGER", None),
                ("frequency_unit", "TEXT", None),
                ("publish_at", "DATETIME", None),
            ]
            
            for col, col_type, default in columns_to_add:
                if col not in course_columns:
                    default_str = f" DEFAULT {default}" if default else ""
                    await cursor.execute(
                        f"ALTER TABLE {course_cohorts_table_name} ADD COLUMN {col} {col_type}{default_str}"
                    )
                    logger.info(f"Added column {col} to {course_cohorts_table_name}")

            await conn.commit()
            logger.info("Table migration completed successfully")

    except Exception as e:
        logger.error(f"Table cleanup/migration failed: {str(e)}")
        raise


# Optional: Add a verification function to check database integrity
async def verify_database_integrity():
    """Verify all required tables exist and have correct structure"""
    required_tables = [
        organizations_table_name,
        org_api_keys_table_name,
        users_table_name,
        user_organizations_table_name,
        cohorts_table_name,
        user_cohorts_table_name,
        courses_table_name,
        course_cohorts_table_name,
        milestones_table_name,
        course_milestones_table_name,
        tasks_table_name,
        course_tasks_table_name,
        questions_table_name,
        scorecards_table_name,
        question_scorecards_table_name,
        chat_history_table_name,
        task_completions_table_name,
        course_generation_jobs_table_name,
        task_generation_jobs_table_name,
        code_drafts_table_name,
    ]
    
    try:
        async with get_new_db_connection() as conn:
            cursor = await conn.cursor()
            
            missing_tables = []
            for table in required_tables:
                if not await check_table_exists(table, cursor):
                    missing_tables.append(table)
            
            if missing_tables:
                logger.error(f"Missing tables: {missing_tables}")
                return False
            
            logger.info("Database integrity check passed - all tables present")
            return True
            
    except Exception as e:
        logger.error(f"Database integrity check failed: {str(e)}")
        return False