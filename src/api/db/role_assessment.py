# src/api/db/role_assessment.py
import json
import uuid
from datetime import datetime
from typing import List, Dict, Optional, Any
from api.utils.db import get_new_db_connection, execute_db_operation, execute_many_db_operation
from api.config import (
    organizations_table_name,
    users_table_name,
    courses_table_name,
    user_organizations_table_name,
)
from api.models import TaskStatus
from api.utils.logging import logger

# Define table names
role_assessments_table_name = "role_assessments"
role_assessment_questions_table_name = "role_assessment_questions"
course_assessments_table_name = "course_assessments"

async def create_role_assessment_tables(cursor):
    """Create tables for role assessments"""
    
    # Main assessments table
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {role_assessments_table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            assessment_id TEXT NOT NULL UNIQUE,
            org_id INTEGER NOT NULL,
            role_name TEXT NOT NULL,
            target_skills TEXT NOT NULL,
            difficulty_level TEXT NOT NULL,
            total_questions INTEGER NOT NULL,
            estimated_duration_minutes INTEGER NOT NULL,
            skill_coverage TEXT,
            created_by INTEGER NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            is_published BOOLEAN DEFAULT FALSE,
            FOREIGN KEY (org_id) REFERENCES {organizations_table_name}(id) ON DELETE CASCADE,
            FOREIGN KEY (created_by) REFERENCES {users_table_name}(id)
        )"""
    )
    
    await cursor.execute(
        f"""CREATE INDEX idx_assessment_org_id ON {role_assessments_table_name} (org_id)"""
    )
    
    await cursor.execute(
        f"""CREATE INDEX idx_assessment_assessment_id ON {role_assessments_table_name} (assessment_id)"""
    )
    
    # Questions table with flexible JSON storage
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {role_assessment_questions_table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            assessment_id TEXT NOT NULL,
            question_type TEXT NOT NULL,
            question_data TEXT NOT NULL,
            position INTEGER NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (assessment_id) REFERENCES {role_assessments_table_name}(assessment_id) ON DELETE CASCADE
        )"""
    )
    
    await cursor.execute(
        f"""CREATE INDEX idx_question_assessment_id ON {role_assessment_questions_table_name} (assessment_id)"""
    )
    
    # Link assessments to courses
    await cursor.execute(
        f"""CREATE TABLE IF NOT EXISTS {course_assessments_table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            course_id INTEGER NOT NULL,
            assessment_id TEXT NOT NULL,
            is_deployed BOOLEAN DEFAULT TRUE,
            deployed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            deployed_by INTEGER NOT NULL,
            position INTEGER DEFAULT 0,
            UNIQUE(course_id, assessment_id),
            FOREIGN KEY (course_id) REFERENCES {courses_table_name}(id) ON DELETE CASCADE,
            FOREIGN KEY (assessment_id) REFERENCES {role_assessments_table_name}(assessment_id) ON DELETE CASCADE,
            FOREIGN KEY (deployed_by) REFERENCES {users_table_name}(id)
        )"""
    )
    
    await cursor.execute(
        f"""CREATE INDEX idx_course_assessment_course_id ON {course_assessments_table_name} (course_id)"""
    )

async def save_assessment(assessment_data: Dict, org_id: int, user_id: int) -> str:
    """Save or update a role assessment"""
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()
    
        assessment_id = assessment_data.get("assessment_id", f"role_assessment_{uuid.uuid4().hex[:8]}")
        
        # Check if assessment exists
        existing = await execute_db_operation(
            f"SELECT id FROM {role_assessments_table_name} WHERE assessment_id = ?",
            (assessment_id,),
            fetch_one=True
        )
        
        # Prepare data
        target_skills = json.dumps(assessment_data.get("target_skills", []))
        skill_coverage = json.dumps(assessment_data.get("skill_coverage", []))
        
        if existing:
            # Update existing assessment
            await cursor.execute(
                f"""UPDATE {role_assessments_table_name} 
                SET role_name = ?, target_skills = ?, difficulty_level = ?, 
                    total_questions = ?, estimated_duration_minutes = ?, 
                    skill_coverage = ?, updated_at = CURRENT_TIMESTAMP
                WHERE assessment_id = ?""",
                (
                    assessment_data["role_name"],
                    target_skills,
                    assessment_data["difficulty_level"],
                    assessment_data["total_questions"],
                    assessment_data["estimated_duration_minutes"],
                    skill_coverage,
                    assessment_id
                )
            )
            
            # Delete existing questions to replace with updated ones
            await cursor.execute(
                f"DELETE FROM {role_assessment_questions_table_name} WHERE assessment_id = ?",
                (assessment_id,)
            )
        else:
            # Create new assessment
            await cursor.execute(
                f"""INSERT INTO {role_assessments_table_name} 
                (assessment_id, org_id, role_name, target_skills, difficulty_level, 
                 total_questions, estimated_duration_minutes, skill_coverage, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    assessment_id,
                    org_id,
                    assessment_data["role_name"],
                    target_skills,
                    assessment_data["difficulty_level"],
                    assessment_data["total_questions"],
                    assessment_data["estimated_duration_minutes"],
                    skill_coverage,
                    user_id
                )
            )
        
        # Save questions
        position = 0
        
        # Save MCQs
        for mcq in assessment_data.get("mcqs", []):
            await cursor.execute(
                f"""INSERT INTO {role_assessment_questions_table_name} 
                (assessment_id, question_type, question_data, position)
                VALUES (?, ?, ?, ?)""",
                (assessment_id, "mcq", json.dumps(mcq), position)
            )
            position += 1
        
        # Save SAQs
        for saq in assessment_data.get("saqs", []):
            await cursor.execute(
                f"""INSERT INTO {role_assessment_questions_table_name} 
                (assessment_id, question_type, question_data, position)
                VALUES (?, ?, ?, ?)""",
                (assessment_id, "saq", json.dumps(saq), position)
            )
            position += 1
        
        # Save Case Study
        if assessment_data.get("case_study"):
            await cursor.execute(
                f"""INSERT INTO {role_assessment_questions_table_name} 
                (assessment_id, question_type, question_data, position)
                VALUES (?, ?, ?, ?)""",
                (assessment_id, "case_study", json.dumps(assessment_data["case_study"]), position)
            )
            position += 1
        
        # Save Aptitude Questions
        for apt in assessment_data.get("aptitude_questions", []):
            await cursor.execute(
                f"""INSERT INTO {role_assessment_questions_table_name} 
                (assessment_id, question_type, question_data, position)
                VALUES (?, ?, ?, ?)""",
                (assessment_id, "aptitude", json.dumps(apt), position)
            )
            position += 1
        
        await conn.commit()
        return assessment_id

async def get_assessment(assessment_id: str) -> Optional[Dict]:
    """Get a role assessment by ID"""
    
    # Get assessment metadata
    assessment = await execute_db_operation(
        f"""SELECT * FROM {role_assessments_table_name} WHERE assessment_id = ?""",
        (assessment_id,),
        fetch_one=True
    )
    
    if not assessment:
        return None
    
    # Get questions
    questions = await execute_db_operation(
        f"""SELECT question_type, question_data, position 
        FROM {role_assessment_questions_table_name} 
        WHERE assessment_id = ? 
        ORDER BY position""",
        (assessment_id,),
        fetch_all=True
    )
    
    # Parse and organize questions
    mcqs = []
    saqs = []
    case_study = None
    aptitude_questions = []
    
    for q_type, q_data, _ in questions:
        data = json.loads(q_data)
        if q_type == "mcq":
            mcqs.append(data)
        elif q_type == "saq":
            saqs.append(data)
        elif q_type == "case_study":
            case_study = data
        elif q_type == "aptitude":
            aptitude_questions.append(data)
    
    return {
        "assessment_id": assessment[1],
        "role_name": assessment[3],
        "target_skills": json.loads(assessment[4]),
        "difficulty_level": assessment[5],
        "total_questions": assessment[6],
        "estimated_duration_minutes": assessment[7],
        "skill_coverage": json.loads(assessment[8]) if assessment[8] else [],
        "mcqs": mcqs,
        "saqs": saqs,
        "case_study": case_study,
        "aptitude_questions": aptitude_questions,
        "created_at": assessment[10],
        "updated_at": assessment[11],
        "is_published": assessment[12]
    }
from typing import List, Dict
import json

async def list_assessments(org_id: int, user_id: int = None) -> List[Dict]:
    """List all assessments for an organization"""
    print(org_id)
    print(user_id)
    query = f"""
        SELECT 
            a.id,
            a.assessment_id,
            a.org_id,
            a.role_name,
            a.target_skills,
            a.difficulty_level,
            a.total_questions,
            a.estimated_duration_minutes,
            a.created_by,
            a.created_by, -- needed to align indexes
            a.created_at,
            a.updated_at,
            a.is_published,
            u.email as created_by_email,
            COUNT(DISTINCT ca.course_id) as deployed_courses_count
        FROM {role_assessments_table_name} a
        LEFT JOIN {users_table_name} u ON a.created_by = u.id
        LEFT JOIN {course_assessments_table_name} ca ON a.assessment_id = ca.assessment_id
        WHERE a.org_id = ?
        GROUP BY a.id
        ORDER BY a.created_at DESC
    """

    assessments = await execute_db_operation(query, (org_id,), fetch_all=True)

    result = []
    for a in assessments:
        result.append({
            "assessment_id": a[1],
            "role_name": a[3],
            "target_skills": json.loads(a[4]) if a[4] else [],
            "difficulty_level": a[5],
            "total_questions": a[6],
            "estimated_duration_minutes": a[7],
            "created_by_email": a[13],
            "created_at": a[10],
            "updated_at": a[11],
            "is_published": a[12],
            "deployed_courses_count": a[14]
        })

    return result


async def deploy_assessment_to_course(assessment_id: str, course_id: int, user_id: int) -> bool:
    """Deploy an assessment to a course"""
    
    async with get_new_db_connection() as conn:
        cursor = await conn.cursor()
        
        # Check if already deployed
        existing = await execute_db_operation(
            f"SELECT id, is_deployed FROM {course_assessments_table_name} WHERE course_id = ? AND assessment_id = ?",
            (course_id, assessment_id),
            fetch_one=True
        )
        
        if existing:
            if existing[1]:  # Already deployed
                return False
            else:  # Was undeployed, redeploy it
                await cursor.execute(
                    f"""UPDATE {course_assessments_table_name} 
                    SET is_deployed = TRUE, deployed_at = CURRENT_TIMESTAMP, deployed_by = ?
                    WHERE course_id = ? AND assessment_id = ?""",
                    (user_id, course_id, assessment_id)
                )
        else:
            # Get next position
            max_position = await execute_db_operation(
                f"SELECT MAX(position) FROM {course_assessments_table_name} WHERE course_id = ?",
                (course_id,),
                fetch_one=True
            )
            next_position = (max_position[0] or 0) + 1 if max_position else 1
            
            # Deploy to course
            await cursor.execute(
                f"""INSERT INTO {course_assessments_table_name} 
                (course_id, assessment_id, deployed_by, position)
                VALUES (?, ?, ?, ?)""",
                (course_id, assessment_id, user_id, next_position)
            )
        
        # Mark assessment as published
        await cursor.execute(
            f"UPDATE {role_assessments_table_name} SET is_published = TRUE WHERE assessment_id = ?",
            (assessment_id,)
        )
        
        await conn.commit()
        return True

async def undeploy_assessment_from_course(assessment_id: str, course_id: int) -> bool:
    """Undeploy an assessment from a course"""
    
    result = await execute_db_operation(
        f"""UPDATE {course_assessments_table_name} 
        SET is_deployed = FALSE 
        WHERE course_id = ? AND assessment_id = ?""",
        (course_id, assessment_id)
    )
    
    return True

async def get_course_assessments(course_id: int) -> List[Dict]:
    """Get all assessments deployed to a course"""
    
    query = f"""
        SELECT a.*, ca.position, ca.is_deployed, ca.deployed_at
        FROM {course_assessments_table_name} ca
        JOIN {role_assessments_table_name} a ON ca.assessment_id = a.assessment_id
        WHERE ca.course_id = ? AND ca.is_deployed = TRUE
        ORDER BY ca.position
    """
    
    assessments = await execute_db_operation(query, (course_id,), fetch_all=True)
    
    result = []
    for assessment in assessments:
        result.append({
            "assessment_id": assessment[1],
            "role_name": assessment[3],
            "target_skills": json.loads(assessment[4]),
            "difficulty_level": assessment[5],
            "total_questions": assessment[6],
            "estimated_duration_minutes": assessment[7],
            "position": assessment[13],
            "deployed_at": assessment[15]
        })
    
    return result

from typing import List, Dict

async def get_courses_for_assessment(assessment_id: str) -> List[Dict]:
    """Get all courses to which a specific assessment has been deployed"""
    
    query = f"""
        SELECT ca.course_id, c.name, ca.position, ca.deployed_at
        FROM {course_assessments_table_name} ca
        JOIN {courses_table_name} c ON ca.course_id = c.id
        WHERE ca.assessment_id = ? AND ca.is_deployed = TRUE
        ORDER BY ca.position
    """

    courses = await execute_db_operation(query, (assessment_id,), fetch_all=True)

    result = []
    for course in courses:
        result.append({
            "course_id": course[0],
            "course_name": course[1],
            "position": course[2],
            "deployed_at": course[3]
        })

    return result


async def get_mentor_courses(user_id: int, org_id: int) -> List[Dict]:
    """Get courses where user is a mentor"""
    
    query = f"""
        SELECT DISTINCT c.id, c.name
        FROM {courses_table_name} c
        WHERE c.org_id = ?
        ORDER BY c.name
    """
    
    # For now, return all courses in the org if user is admin/owner
    # You can modify this to check actual mentor role in cohorts
    courses = await execute_db_operation(query, (org_id,), fetch_all=True)
    
    return [{"id": c[0], "name": c[1]} for c in courses]

