import json
import sqlite3
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
from collections import defaultdict
import statistics

from . import (
    execute_db_operation,
    assessment_submissions_table_name,
    question_responses_table_name,
    leaderboard_entries_table_name,
    assessment_analytics_table_name,
    tasks_table_name,
    questions_table_name,
    users_table_name,
    cohorts_table_name,
    courses_table_name
)
from ..models import (
    AssessmentSubmission, AssessmentStatus, QuestionResponse, ResponseType,
    AssessmentTask, AssessmentQuestion, AssessmentSession, StudentAssessmentResult,
    AssessmentAnalytics, LeaderboardEntry, InstructorAssessmentOverview,
    StudentSummary, QuestionAnalytics
)

# Assessment submission functions
async def start_assessment(user_id: int, task_id: int, cohort_id: Optional[int] = None, 
                          course_id: Optional[int] = None) -> AssessmentSubmission:
    """Start a new assessment session for a user"""
    
    # Check if user has an existing in-progress submission
    existing = await execute_db_operation(
        f"""SELECT id FROM {assessment_submissions_table_name} 
           WHERE user_id = ? AND task_id = ? AND status = ?""",
        (user_id, task_id, AssessmentStatus.IN_PROGRESS.value),
        fetch_one=True
    )
    
    if existing:
        # Return existing submission
        return await get_assessment_submission(existing[0])
    
    # Get current attempt number
    attempt_count = await execute_db_operation(
        f"""SELECT COUNT(*) FROM {assessment_submissions_table_name} 
           WHERE user_id = ? AND task_id = ?""",
        (user_id, task_id),
        fetch_one=True
    )
    attempt_number = (attempt_count[0] if attempt_count else 0) + 1
    
    # Create new submission
    submission_id = await execute_db_operation(
        f"""INSERT INTO {assessment_submissions_table_name} 
           (user_id, task_id, cohort_id, course_id, attempt_number, status)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (user_id, task_id, cohort_id, course_id, attempt_number, AssessmentStatus.IN_PROGRESS.value),
        fetch_lastrowid=True
    )
    
    return await get_assessment_submission(submission_id)

async def get_assessment_submission(submission_id: int) -> AssessmentSubmission:
    """Get assessment submission by ID"""
    result = await execute_db_operation(
        f"""SELECT id, user_id, task_id, cohort_id, course_id, started_at, submitted_at,
                  time_spent_seconds, total_score, max_possible_score, percentage_score,
                  status, attempt_number, is_final_submission, metadata
           FROM {assessment_submissions_table_name} WHERE id = ?""",
        (submission_id,),
        fetch_one=True
    )
    
    if not result:
        raise ValueError(f"Assessment submission {submission_id} not found")
    
    metadata = json.loads(result[13]) if result[13] else {}
    
    return AssessmentSubmission(
        id=result[0],
        user_id=result[1],
        task_id=result[2],
        cohort_id=result[3],
        course_id=result[4],
        started_at=datetime.fromisoformat(result[5]),
        submitted_at=datetime.fromisoformat(result[6]) if result[6] else None,
        time_spent_seconds=result[7],
        total_score=result[8],
        max_possible_score=result[9],
        percentage_score=result[10],
        status=AssessmentStatus(result[11]),
        attempt_number=result[12],
        is_final_submission=bool(result[12]),
        metadata=metadata
    )

async def get_assessment_task(task_id: int) -> AssessmentTask:
    """Get task details with questions for assessment taking"""
    
    # Get task details
    task_result = await execute_db_operation(
        f"""SELECT id, title, type FROM {tasks_table_name} WHERE id = ?""",
        (task_id,),
        fetch_one=True
    )
    
    if not task_result:
        raise ValueError(f"Task {task_id} not found")
    
    # Get questions
    questions_result = await execute_db_operation(
        f"""SELECT id, title, blocks, type, input_type, response_type, coding_languages,
                  max_attempts, is_feedback_shown, scorecard_id, position
           FROM {questions_table_name} 
           WHERE task_id = ? AND deleted_at IS NULL 
           ORDER BY position ASC""",
        (task_id,),
        fetch_all=True
    )
    
    questions = []
    for q in questions_result:
        questions.append(AssessmentQuestion(
            id=q[0],
            title=q[1],
            blocks=json.loads(q[2]) if q[2] else [],
            type=q[3],
            input_type=q[4],
            response_type=q[5],
            coding_languages=json.loads(q[6]) if q[6] else None,
            max_attempts=q[7],
            is_feedback_shown=bool(q[8]) if q[8] is not None else None,
            scorecard=None,  # TODO: Load scorecard if needed
            position=q[10] or 0
        ))
    
    return AssessmentTask(
        id=task_result[0],
        title=task_result[1],
        type=task_result[2],
        questions=questions,
        total_questions=len(questions),
        estimated_time_minutes=None,  # TODO: Calculate based on questions
        instructions=None,
        is_timed=False,
        time_limit_minutes=None
    )

async def submit_question_response(submission_id: int, question_id: int, 
                                 user_response: str, response_type: ResponseType,
                                 time_spent_seconds: int = 0) -> QuestionResponse:
    """Submit or update a response to a specific question"""
    
    # Check if response already exists
    existing = await execute_db_operation(
        f"""SELECT id FROM {question_responses_table_name} 
           WHERE submission_id = ? AND question_id = ?""",
        (submission_id, question_id),
        fetch_one=True
    )
    
    if existing:
        # Update existing response
        await execute_db_operation(
            f"""UPDATE {question_responses_table_name} 
               SET user_response = ?, user_response_type = ?, time_spent_seconds = ?,
                   attempt_count = attempt_count + 1, updated_at = CURRENT_TIMESTAMP
               WHERE id = ?""",
            (user_response, response_type.value, time_spent_seconds, existing[0])
        )
        response_id = existing[0]
    else:
        # Create new response
        response_id = await execute_db_operation(
            f"""INSERT INTO {question_responses_table_name}
               (submission_id, question_id, user_response, user_response_type, time_spent_seconds)
               VALUES (?, ?, ?, ?, ?)""",
            (submission_id, question_id, user_response, response_type.value, time_spent_seconds),
            fetch_lastrowid=True
        )
    
    # Update submission time spent
    await execute_db_operation(
        f"""UPDATE {assessment_submissions_table_name} 
           SET time_spent_seconds = time_spent_seconds + ?, updated_at = CURRENT_TIMESTAMP
           WHERE id = ?""",
        (time_spent_seconds, submission_id)
    )
    
    return await get_question_response(response_id)

async def get_question_response(response_id: int) -> QuestionResponse:
    """Get question response by ID"""
    result = await execute_db_operation(
        f"""SELECT id, submission_id, question_id, user_response, user_response_type,
                  ai_feedback, score, max_score, is_correct, time_spent_seconds,
                  attempt_count, submitted_at, graded_at, graded_by, scorecard_results
           FROM {question_responses_table_name} WHERE id = ?""",
        (response_id,),
        fetch_one=True
    )
    
    if not result:
        raise ValueError(f"Question response {response_id} not found")
    
    return QuestionResponse(
        id=result[0],
        submission_id=result[1],
        question_id=result[2],
        user_response=result[3],
        user_response_type=ResponseType(result[4]),
        ai_feedback=result[5],
        score=result[6],
        max_score=result[7],
        is_correct=bool(result[8]) if result[8] is not None else None,
        time_spent_seconds=result[9],
        attempt_count=result[10],
        submitted_at=datetime.fromisoformat(result[11]) if result[11] else None,
        graded_at=datetime.fromisoformat(result[12]) if result[12] else None,
        graded_by=result[13] if result[13] else "ai",
        scorecard_results=json.loads(result[14]) if result[14] else None
    )

async def finalize_assessment_submission(submission_id: int) -> AssessmentSubmission:
    """Finalize an assessment submission and calculate final scores"""
    
    # Get all question responses for this submission
    responses = await execute_db_operation(
        f"""SELECT id, score, max_score FROM {question_responses_table_name}
           WHERE submission_id = ?""",
        (submission_id,),
        fetch_all=True
    )
    
    # Calculate total scores
    total_score = sum(r[1] for r in responses)
    max_possible_score = sum(r[2] for r in responses)
    percentage_score = (total_score / max_possible_score * 100) if max_possible_score > 0 else 0
    
    # Update submission
    await execute_db_operation(
        f"""UPDATE {assessment_submissions_table_name}
           SET submitted_at = CURRENT_TIMESTAMP, total_score = ?, max_possible_score = ?,
               percentage_score = ?, status = ?, is_final_submission = TRUE,
               updated_at = CURRENT_TIMESTAMP
           WHERE id = ?""",
        (total_score, max_possible_score, percentage_score, AssessmentStatus.SUBMITTED.value, submission_id)
    )
    
    # Update leaderboard
    await update_leaderboard_entry(submission_id)
    
    return await get_assessment_submission(submission_id)

async def update_leaderboard_entry(submission_id: int):
    """Update or create leaderboard entry for a submission"""
    
    # Get submission details
    submission = await get_assessment_submission(submission_id)
    
    # Get user details
    user_result = await execute_db_operation(
        f"""SELECT name, email FROM {users_table_name} WHERE id = ?""",
        (submission.user_id,),
        fetch_one=True
    )
    
    if not user_result:
        return
    
    # Check if entry exists
    existing = await execute_db_operation(
        f"""SELECT id FROM {leaderboard_entries_table_name}
           WHERE user_id = ? AND task_id = ?""",
        (submission.user_id, submission.task_id),
        fetch_one=True
    )
    
    if existing:
        # Update existing entry (keep best score)
        await execute_db_operation(
            f"""UPDATE {leaderboard_entries_table_name}
               SET score = MAX(score, ?), percentage = MAX(percentage, ?),
                   submission_id = CASE WHEN ? > percentage THEN ? ELSE submission_id END,
                   updated_at = CURRENT_TIMESTAMP
               WHERE id = ?""",
            (submission.total_score, submission.percentage_score,
             submission.percentage_score, submission_id, existing[0])
        )
    else:
        # Create new entry
        await execute_db_operation(
            f"""INSERT INTO {leaderboard_entries_table_name}
               (user_id, cohort_id, course_id, task_id, score, max_score, percentage, submission_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (submission.user_id, submission.cohort_id, submission.course_id,
             submission.task_id, submission.total_score, submission.max_possible_score,
             submission.percentage_score, submission_id)
        )
    
    # Update rankings
    await update_leaderboard_rankings(submission.task_id, submission.cohort_id)

async def update_leaderboard_rankings(task_id: int, cohort_id: Optional[int] = None):
    """Update rank positions for a task leaderboard"""
    
    # Get all entries for this task/cohort ordered by percentage
    where_clause = "task_id = ?"
    params = [task_id]
    
    if cohort_id:
        where_clause += " AND cohort_id = ?"
        params.append(cohort_id)
    
    entries = await execute_db_operation(
        f"""SELECT id, percentage FROM {leaderboard_entries_table_name}
           WHERE {where_clause} ORDER BY percentage DESC, score DESC""",
        params,
        fetch_all=True
    )
    
    # Update rankings
    for rank, (entry_id, _) in enumerate(entries, 1):
        await execute_db_operation(
            f"""UPDATE {leaderboard_entries_table_name}
               SET rank_position = ?, updated_at = CURRENT_TIMESTAMP
               WHERE id = ?""",
            (rank, entry_id)
        )

# Analytics functions
async def get_assessment_analytics(task_id: int, cohort_id: Optional[int] = None) -> AssessmentAnalytics:
    """Get comprehensive analytics for an assessment"""
    
    # Get task details
    task_result = await execute_db_operation(
        f"""SELECT title FROM {tasks_table_name} WHERE id = ?""",
        (task_id,),
        fetch_one=True
    )
    
    if not task_result:
        raise ValueError(f"Task {task_id} not found")
    
    # Get submissions for this task
    where_clause = "task_id = ? AND status = ?"
    params = [task_id, AssessmentStatus.SUBMITTED.value]
    
    if cohort_id:
        where_clause += " AND cohort_id = ?"
        params.append(cohort_id)
    
    submissions = await execute_db_operation(
        f"""SELECT total_score, percentage_score, time_spent_seconds
           FROM {assessment_submissions_table_name}
           WHERE {where_clause}""",
        params,
        fetch_all=True
    )
    
    if not submissions:
        # Return empty analytics
        return AssessmentAnalytics(
            task_id=task_id,
            task_title=task_result[0],
            total_submissions=0,
            avg_score=0.0,
            median_score=0.0,
            highest_score=0.0,
            lowest_score=0.0,
            avg_time_minutes=0.0,
            completion_rate=0.0,
            difficulty_rating=0.0,
            question_analytics=[],
            score_distribution={},
            last_calculated=datetime.now()
        )
    
    # Calculate basic statistics
    scores = [s[1] for s in submissions]  # percentage scores
    time_minutes = [s[2] / 60 for s in submissions]  # convert to minutes
    
    avg_score = statistics.mean(scores)
    median_score = statistics.median(scores)
    highest_score = max(scores)
    lowest_score = min(scores)
    avg_time = statistics.mean(time_minutes)
    
    # Calculate score distribution
    score_distribution = {
        "90-100": len([s for s in scores if s >= 90]),
        "80-89": len([s for s in scores if 80 <= s < 90]),
        "70-79": len([s for s in scores if 70 <= s < 80]),
        "60-69": len([s for s in scores if 60 <= s < 70]),
        "0-59": len([s for s in scores if s < 60])
    }
    
    # Calculate difficulty rating (inverse of average score)
    difficulty_rating = max(0, (100 - avg_score) / 100)
    
    # Get question-level analytics
    question_analytics = await get_question_analytics(task_id, cohort_id)
    
    return AssessmentAnalytics(
        task_id=task_id,
        task_title=task_result[0],
        total_submissions=len(submissions),
        avg_score=avg_score,
        median_score=median_score,
        highest_score=highest_score,
        lowest_score=lowest_score,
        avg_time_minutes=avg_time,
        completion_rate=100.0,  # TODO: Calculate actual completion rate
        difficulty_rating=difficulty_rating,
        question_analytics=question_analytics,
        score_distribution=score_distribution,
        last_calculated=datetime.now()
    )

async def get_question_analytics(task_id: int, cohort_id: Optional[int] = None) -> List[QuestionAnalytics]:
    """Get analytics for individual questions in an assessment"""
    
    # Get questions for this task
    questions = await execute_db_operation(
        f"""SELECT id, title FROM {questions_table_name}
           WHERE task_id = ? AND deleted_at IS NULL
           ORDER BY position ASC""",
        (task_id,),
        fetch_all=True
    )
    
    analytics = []
    
    for question_id, question_title in questions:
        # Get responses for this question
        where_clause = f"""qr.question_id = ? AND s.status = ?"""
        params = [question_id, AssessmentStatus.SUBMITTED.value]
        
        if cohort_id:
            where_clause += " AND s.cohort_id = ?"
            params.append(cohort_id)
        
        responses = await execute_db_operation(
            f"""SELECT qr.score, qr.max_score, qr.is_correct, qr.time_spent_seconds
               FROM {question_responses_table_name} qr
               JOIN {assessment_submissions_table_name} s ON qr.submission_id = s.id
               WHERE {where_clause}""",
            params,
            fetch_all=True
        )
        
        if responses:
            total_responses = len(responses)
            correct_responses = len([r for r in responses if r[2]])
            avg_score = statistics.mean([r[0] for r in responses])
            avg_time = statistics.mean([r[3] for r in responses])
            
            # Determine difficulty level
            if avg_score >= 80:
                difficulty = "easy"
            elif avg_score >= 60:
                difficulty = "medium"
            else:
                difficulty = "hard"
            
            analytics.append(QuestionAnalytics(
                question_id=question_id,
                question_title=question_title,
                total_responses=total_responses,
                correct_responses=correct_responses,
                average_score=avg_score,
                average_time_seconds=int(avg_time),
                difficulty_level=difficulty,
                common_mistakes=[]  # TODO: Implement common mistake analysis
            ))
    
    return analytics

async def get_leaderboard(task_id: int, cohort_id: Optional[int] = None, limit: int = 50) -> List[LeaderboardEntry]:
    """Get leaderboard for an assessment"""
    
    where_clause = "le.task_id = ?"
    params = [task_id]
    
    if cohort_id:
        where_clause += " AND le.cohort_id = ?"
        params.append(cohort_id)
    
    results = await execute_db_operation(
        f"""SELECT le.id, le.user_id, u.name, u.email, le.score, le.max_score,
                  le.percentage, le.rank_position, le.submission_id, le.achievement_badges,
                  s.submitted_at, s.time_spent_seconds
           FROM {leaderboard_entries_table_name} le
           JOIN {users_table_name} u ON le.user_id = u.id
           LEFT JOIN {assessment_submissions_table_name} s ON le.submission_id = s.id
           WHERE {where_clause}
           ORDER BY le.rank_position ASC
           LIMIT ?""",
        params + [limit],
        fetch_all=True
    )
    
    entries = []
    for result in results:
        entries.append(LeaderboardEntry(
            id=result[0],
            user_id=result[1],
            user_name=result[2] or "Unknown",
            user_email=result[3] or "",
            score=result[4],
            max_score=result[5],
            percentage=result[6],
            rank_position=result[7] or 0,
            submission_id=result[8],
            achievement_badges=json.loads(result[9]) if result[9] else [],
            submitted_at=datetime.fromisoformat(result[10]) if result[10] else None,
            time_spent_minutes=result[11] / 60 if result[11] else 0
        ))
    
    return entries

async def get_student_assessment_result(submission_id: int) -> StudentAssessmentResult:
    """Get detailed results for a student's assessment submission"""
    
    # Get submission details
    submission = await get_assessment_submission(submission_id)
    
    # Get task details
    task_result = await execute_db_operation(
        f"""SELECT title FROM {tasks_table_name} WHERE id = ?""",
        (submission.task_id,),
        fetch_one=True
    )
    
    if not task_result:
        raise ValueError(f"Task {submission.task_id} not found")
    
    # Get question responses
    responses = await execute_db_operation(
        f"""SELECT qr.question_id, q.title, qr.user_response, qr.ai_feedback,
                  qr.score, qr.max_score, qr.is_correct, qr.time_spent_seconds,
                  qr.scorecard_results
           FROM {question_responses_table_name} qr
           JOIN {questions_table_name} q ON qr.question_id = q.id
           WHERE qr.submission_id = ?
           ORDER BY q.position ASC""",
        (submission_id,),
        fetch_all=True
    )
    
    question_results = []
    for r in responses:
        percentage = (r[4] / r[5] * 100) if r[5] > 0 else 0
        
        question_results.append({
            "question_id": r[0],
            "question_title": r[1],
            "user_response": r[2] or "",
            "correct_answer": None,  # TODO: Get from question data
            "ai_feedback": r[3] or "",
            "score": r[4],
            "max_score": r[5],
            "percentage": percentage,
            "is_correct": r[6],
            "time_spent_seconds": r[7],
            "scorecard_breakdown": json.loads(r[8]) if r[8] else None
        })
    
    # Calculate grade letter
    grade_letter = "F"
    if submission.percentage_score >= 90:
        grade_letter = "A"
    elif submission.percentage_score >= 80:
        grade_letter = "B"
    elif submission.percentage_score >= 70:
        grade_letter = "C"
    elif submission.percentage_score >= 60:
        grade_letter = "D"
    
    # Get rank in cohort if cohort_id exists
    rank_in_cohort = None
    total_participants = None
    
    if submission.cohort_id:
        rank_result = await execute_db_operation(
            f"""SELECT rank_position FROM {leaderboard_entries_table_name}
               WHERE task_id = ? AND cohort_id = ? AND user_id = ?""",
            (submission.task_id, submission.cohort_id, submission.user_id),
            fetch_one=True
        )
        
        if rank_result:
            rank_in_cohort = rank_result[0]
        
        # Get total participants
        total_result = await execute_db_operation(
            f"""SELECT COUNT(*) FROM {leaderboard_entries_table_name}
               WHERE task_id = ? AND cohort_id = ?""",
            (submission.task_id, submission.cohort_id),
            fetch_one=True
        )
        
        if total_result:
            total_participants = total_result[0]
    
    return {
        "submission_id": submission_id,
        "task_title": task_result[0],
        "total_score": submission.total_score,
        "max_possible_score": submission.max_possible_score,
        "percentage_score": submission.percentage_score,
        "grade_letter": grade_letter,
        "rank_in_cohort": rank_in_cohort,
        "total_cohort_participants": total_participants,
        "time_spent_minutes": submission.time_spent_seconds / 60,
        "submitted_at": submission.submitted_at,
        "question_results": question_results,
        "overall_feedback": "Great work! Keep practicing to improve further.",  # TODO: Generate AI feedback
        "areas_for_improvement": [],  # TODO: Analyze weak areas
        "strengths": []  # TODO: Analyze strong areas
    }