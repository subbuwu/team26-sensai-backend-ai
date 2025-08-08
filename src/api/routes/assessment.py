from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
import asyncio

from ..models import (
    StartAssessmentRequest, SubmitQuestionRequest, FinalizeAssessmentRequest,
    AssessmentSubmission, AssessmentSubmissionWithResponses, QuestionResponse,
    AssessmentTask, AssessmentSession, StudentAssessmentResult,
    AssessmentAnalytics, LeaderboardEntry, InstructorAssessmentOverview,
    AssessmentLeaderboard, ResponseType
)
from ..db.assessment import (
    start_assessment, get_assessment_submission, get_assessment_task,
    submit_question_response, finalize_assessment_submission,
    get_assessment_analytics, get_leaderboard, get_student_assessment_result,
    get_question_response
)
from ..db.user import get_current_user

router = APIRouter()

# Assessment taking endpoints
@router.post("/start", response_model=AssessmentSession)
async def start_assessment_session(
    request: StartAssessmentRequest,
    current_user: dict = Depends(get_current_user)
):
    """Start a new assessment session for the current user"""
    try:
        # Start the assessment submission
        submission = await start_assessment(
            user_id=current_user["id"],
            task_id=request.task_id,
            cohort_id=request.cohort_id,
            course_id=request.course_id
        )
        
        # Get task details
        task = await get_assessment_task(request.task_id)
        
        # Create session object
        session = AssessmentSession(
            submission=submission,
            task=task,
            current_question_index=0,
            progress_percentage=0.0,
            can_navigate_freely=True,
            saved_responses={}
        )
        
        return session
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/{submission_id}/session", response_model=AssessmentSession)
async def get_assessment_session(
    submission_id: int,
    current_user: dict = Depends(get_current_user)
):
    """Get existing assessment session"""
    try:
        # Get submission
        submission = await get_assessment_submission(submission_id)
        
        # Verify user owns this submission
        if submission.user_id != current_user["id"]:
            raise HTTPException(status_code=403, detail="Access denied")
        
        # Get task details
        task = await get_assessment_task(submission.task_id)
        
        # TODO: Load saved responses and calculate progress
        session = AssessmentSession(
            submission=submission,
            task=task,
            current_question_index=0,
            progress_percentage=0.0,
            can_navigate_freely=True,
            saved_responses={}
        )
        
        return session
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/question/submit", response_model=QuestionResponse)
async def submit_question_answer(
    request: SubmitQuestionRequest,
    current_user: dict = Depends(get_current_user)
):
    """Submit an answer to a specific question"""
    try:
        # Verify user owns the submission
        submission = await get_assessment_submission(request.submission_id)
        if submission.user_id != current_user["id"]:
            raise HTTPException(status_code=403, detail="Access denied")
        
        # Submit the response
        response = await submit_question_response(
            submission_id=request.submission_id,
            question_id=request.question_id,
            user_response=request.user_response,
            response_type=request.response_type,
            time_spent_seconds=request.time_spent_seconds
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/{submission_id}/finalize", response_model=StudentAssessmentResult)
async def finalize_assessment(
    submission_id: int,
    request: FinalizeAssessmentRequest,
    current_user: dict = Depends(get_current_user)
):
    """Finalize assessment submission and get immediate results"""
    try:
        # Verify user owns the submission
        submission = await get_assessment_submission(submission_id)
        if submission.user_id != current_user["id"]:
            raise HTTPException(status_code=403, detail="Access denied")
        
        if not request.confirm_submission:
            raise HTTPException(status_code=400, detail="Submission not confirmed")
        
        # Finalize the submission
        final_submission = await finalize_assessment_submission(submission_id)
        
        # Get detailed results
        results = await get_student_assessment_result(submission_id)
        
        return results
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Results viewing endpoints
@router.get("/{submission_id}/results", response_model=StudentAssessmentResult)
async def get_assessment_results(
    submission_id: int,
    current_user: dict = Depends(get_current_user)
):
    """Get detailed results for a specific submission"""
    try:
        # Verify user owns the submission or has instructor access
        submission = await get_assessment_submission(submission_id)
        
        # TODO: Add instructor access check for cohort/course
        if submission.user_id != current_user["id"]:
            raise HTTPException(status_code=403, detail="Access denied")
        
        results = await get_student_assessment_result(submission_id)
        return results
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/task/{task_id}/leaderboard", response_model=AssessmentLeaderboard)
async def get_task_leaderboard(
    task_id: int,
    cohort_id: Optional[int] = Query(None),
    limit: int = Query(50, le=100),
    current_user: dict = Depends(get_current_user)
):
    """Get leaderboard for a specific task"""
    try:
        # TODO: Add access control checks
        
        # Get leaderboard entries
        entries = await get_leaderboard(task_id, cohort_id, limit)
        
        # Get task details
        task = await get_assessment_task(task_id)
        
        # Calculate average score
        avg_score = sum(entry.percentage for entry in entries) / len(entries) if entries else 0
        
        # Get cohort name if specified
        cohort_name = None
        if cohort_id:
            # TODO: Get cohort name from database
            cohort_name = f"Cohort {cohort_id}"
        
        leaderboard = AssessmentLeaderboard(
            task_id=task_id,
            task_title=task.title,
            cohort_id=cohort_id,
            cohort_name=cohort_name,
            entries=entries,
            total_participants=len(entries),
            avg_score=avg_score,
            generated_at=datetime.now()
        )
        
        return leaderboard
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Analytics endpoints for instructors
@router.get("/task/{task_id}/analytics", response_model=AssessmentAnalytics)
async def get_task_analytics(
    task_id: int,
    cohort_id: Optional[int] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """Get comprehensive analytics for an assessment task"""
    try:
        # TODO: Verify instructor access to this task/cohort
        
        analytics = await get_assessment_analytics(task_id, cohort_id)
        return analytics
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/cohort/{cohort_id}/task/{task_id}/overview", response_model=InstructorAssessmentOverview)
async def get_instructor_assessment_overview(
    cohort_id: int,
    task_id: int,
    current_user: dict = Depends(get_current_user)
):
    """Get instructor overview of all students' progress on an assessment"""
    try:
        # TODO: Verify instructor access to this cohort
        
        # Get analytics
        analytics = await get_assessment_analytics(task_id, cohort_id)
        
        # Get all students in cohort with their submission status
        # TODO: Implement get_cohort_students_assessment_status function
        students = []  # Placeholder
        
        # Get task and cohort details
        task = await get_assessment_task(task_id)
        
        overview = InstructorAssessmentOverview(
            task_id=task_id,
            task_title=task.title,
            cohort_id=cohort_id,
            cohort_name=f"Cohort {cohort_id}",  # TODO: Get actual cohort name
            total_students=len(students),
            submitted_count=analytics.total_submissions,
            in_progress_count=0,  # TODO: Calculate
            not_started_count=0,  # TODO: Calculate
            avg_score=analytics.avg_score,
            avg_time_minutes=analytics.avg_time_minutes,
            students=students,
            analytics=analytics
        )
        
        return overview
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Student progress endpoints
@router.get("/user/{user_id}/submissions")
async def get_user_submissions(
    user_id: int,
    task_id: Optional[int] = Query(None),
    cohort_id: Optional[int] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """Get all submissions for a user"""
    try:
        # Verify access (own submissions or instructor access)
        if user_id != current_user["id"]:
            # TODO: Verify instructor access
            pass
        
        # TODO: Implement get_user_submissions function
        return {"submissions": []}
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/user/{user_id}/progress")
async def get_user_progress(
    user_id: int,
    current_user: dict = Depends(get_current_user)
):
    """Get overall progress and achievements for a user"""
    try:
        # Verify access
        if user_id != current_user["id"]:
            # TODO: Verify instructor access
            pass
        
        # TODO: Implement get_user_progress function
        return {"progress": {}}
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Auto-grading endpoints
@router.post("/{submission_id}/grade")
async def trigger_auto_grading(
    submission_id: int,
    current_user: dict = Depends(get_current_user)
):
    """Trigger AI auto-grading for a submission"""
    try:
        # TODO: Verify instructor access
        
        # Get submission
        submission = await get_assessment_submission(submission_id)
        
        # TODO: Implement AI grading logic
        # This would involve:
        # 1. Getting all question responses
        # 2. Calling AI models to evaluate subjective questions
        # 3. Updating scores and feedback
        # 4. Recalculating total scores
        
        return {"message": "Auto-grading initiated", "submission_id": submission_id}
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/question/{response_id}/regrade")
async def regrade_question_response(
    response_id: int,
    current_user: dict = Depends(get_current_user)
):
    """Trigger re-grading for a specific question response"""
    try:
        # TODO: Verify instructor access
        
        # Get question response
        response = await get_question_response(response_id)
        
        # TODO: Implement AI re-grading logic
        
        return {"message": "Re-grading initiated", "response_id": response_id}
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Add this to your assessment router or create a new adapter

from ..models import AssessmentTask, AssessmentQuestion
from ..db.role_assessment import get_role_assessment  # Your existing function

@router.get("/role-assessment/{assessment_id}/as-task", response_model=AssessmentTask)
async def convert_role_assessment_to_task(
    assessment_id: int,
    current_user: dict = Depends(get_current_user)
):
    """Convert a role assessment to an assessment task format"""
    try:
        # Get the role assessment
        role_assessment = await get_role_assessment(assessment_id)
        
        if not role_assessment:
            raise HTTPException(status_code=404, detail="Role assessment not found")
        
        # Convert role assessment questions to assessment questions
        questions = []
        question_counter = 1
        
        # Convert MCQs
        if role_assessment.mcqs:
            for mcq in role_assessment.mcqs:
                question = AssessmentQuestion(
                    id=mcq.id,
                    title=f"Question {question_counter}",
                    blocks=[{
                        "id": f"q_{mcq.id}",
                        "type": "paragraph",
                        "content": [{"text": mcq.question}],
                        "props": {},
                        "children": []
                    }],
                    type="objective",
                    input_type="text",
                    response_type="exam",
                    position=question_counter,
                    # Store MCQ options and correct answer in metadata
                    metadata={
                        "question_type": "mcq",
                        "options": mcq.options,
                        "correct_answer": mcq.correct_answer,
                        "explanation": mcq.explanation
                    }
                )
                questions.append(question)
                question_counter += 1
        
        # Convert SAQs
        if role_assessment.saqs:
            for saq in role_assessment.saqs:
                question = AssessmentQuestion(
                    id=saq.id + 10000,  # Offset to avoid ID conflicts
                    title=f"Question {question_counter}",
                    blocks=[{
                        "id": f"q_{saq.id}",
                        "type": "paragraph",
                        "content": [{"text": saq.question}],
                        "props": {},
                        "children": []
                    }],
                    type="subjective",
                    input_type="text",
                    response_type="exam",
                    position=question_counter,
                    metadata={
                        "question_type": "saq",
                        "sample_answer": saq.sample_answer
                    }
                )
                questions.append(question)
                question_counter += 1
        
        # Convert Case Study
        if role_assessment.case_study:
            for i, cs_question in enumerate(role_assessment.case_study.questions):
                question = AssessmentQuestion(
                    id=20000 + i,  # Offset to avoid ID conflicts
                    title=f"Case Study Question {i + 1}",
                    blocks=[
                        {
                            "id": f"case_study_scenario",
                            "type": "heading",
                            "content": [{"text": role_assessment.case_study.title}],
                            "props": {},
                            "children": []
                        },
                        {
                            "id": f"case_study_desc",
                            "type": "paragraph",
                            "content": [{"text": role_assessment.case_study.scenario}],
                            "props": {},
                            "children": []
                        },
                        {
                            "id": f"case_study_q_{i}",
                            "type": "paragraph",
                            "content": [{"text": cs_question}],
                            "props": {},
                            "children": []
                        }
                    ],
                    type="subjective",
                    input_type="text",
                    response_type="exam",
                    position=question_counter,
                    metadata={
                        "question_type": "case_study",
                        "case_study_title": role_assessment.case_study.title,
                        "case_study_scenario": role_assessment.case_study.scenario
                    }
                )
                questions.append(question)
                question_counter += 1
        
        # Convert Aptitude Questions
        if role_assessment.aptitude_questions:
            for apt_q in role_assessment.aptitude_questions:
                question = AssessmentQuestion(
                    id=apt_q.id + 30000,  # Offset to avoid ID conflicts
                    title=f"Question {question_counter}",
                    blocks=[{
                        "id": f"apt_q_{apt_q.id}",
                        "type": "paragraph",
                        "content": [{"text": apt_q.question}],
                        "props": {},
                        "children": []
                    }],
                    type="objective",
                    input_type="text",
                    response_type="exam",
                    position=question_counter,
                    metadata={
                        "question_type": "aptitude",
                        "correct_answer": apt_q.correct_answer,
                        "explanation": apt_q.explanation
                    }
                )
                questions.append(question)
                question_counter += 1
        
        # Create the task
        task = AssessmentTask(
            id=role_assessment.id,
            title=f"{role_assessment.role_name} Assessment",
            type="role_assessment",
            questions=questions,
            total_questions=len(questions),
            estimated_time_minutes=role_assessment.estimated_duration_minutes,
            instructions=f"Complete this {role_assessment.role_name} assessment. Answer all questions to the best of your ability.",
            is_timed=True,
            time_limit_minutes=role_assessment.estimated_duration_minutes,
            metadata={
                "role_name": role_assessment.role_name,
                "difficulty_level": role_assessment.difficulty_level,
                "target_skills": role_assessment.target_skills,
                "original_role_assessment_id": role_assessment.id
            }
        )
        
        return task
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Update the start_assessment function to handle role assessments
async def start_assessment_with_role_support(
    user_id: int,
    task_id: int,
    cohort_id: Optional[int] = None,
    course_id: Optional[int] = None
):
    """Enhanced start_assessment that can handle role assessments"""
    
    # First try to get as regular assessment task
    try:
        task = await get_assessment_task(task_id)
    except:
        # If not found, try to convert from role assessment
        role_assessment = await get_role_assessment(task_id)
        if not role_assessment:
            raise HTTPException(status_code=404, detail="Assessment not found")
        
        # Convert role assessment to task format (similar to above logic)
        # This would be the same conversion logic as in the endpoint above
        pass
    
    # Continue with normal assessment submission creation
    submission = await create_assessment_submission(
        user_id=user_id,
        task_id=task_id,
        cohort_id=cohort_id,
        course_id=course_id
    )
    
    return submission