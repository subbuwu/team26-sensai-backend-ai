# src/api/routes/role_assessment.py
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, status
from typing import List, Dict, Optional, Literal
from pydantic import BaseModel, Field
import json
import uuid
import openai
import os
from api.utils.logging import logger
from api.db.role_assessment import (
    save_assessment,
    get_assessment,
    list_assessments,
    deploy_assessment_to_course,
    undeploy_assessment_from_course,
    get_course_assessments,
    get_mentor_courses,
)
from api.db import get_new_db_connection

load_dotenv()
router = APIRouter()

# Initialize OpenAI client with custom configuration
api_key = os.getenv("OPENAI_API_KEY")
client = openai.OpenAI( 
    api_key = api_key,
    base_url="https://agent.dev.hyperverge.org"
)

# ============================================================================
# SIMPLE USER HELPER (for demo/testing)
# ============================================================================

async def get_default_user(user_id: str | None = None) -> dict:
    """Get a default user for simple testing - replace with your logic"""
    user_id = int(user_id or 1)  # Convert to int

    async with get_new_db_connection() as conn:
        cursor = await conn.execute("""
            SELECT 
                u.id,               
                u.email,            
                uo.org_id,          
                uo.role,            
                u.first_name        
            FROM users u
            LEFT JOIN user_organizations uo ON u.id = uo.user_id
            WHERE u.id = ?
            LIMIT 1;
        """, (user_id,))
        
        user_row = await cursor.fetchone()
        
        if not user_row:
            raise HTTPException(status_code=404, detail="User not found")
        
        return {
            "id": user_row[0],
            "email": user_row[1], 
            "org_id": user_row[2],
            "role": user_row[3],
            "first_name": user_row[4],
        }

# ============================================================================
# PYDANTIC MODELS
# ============================================================================

class GenerateAssessmentRequest(BaseModel):
    role: str = Field(description="Target role name")
    skills: List[str] = Field(description="List of skills to assess")
    difficulty: Literal["easy", "medium", "hard"] = Field(description="Assessment difficulty")

class MCQuestion(BaseModel):
    id: int
    question: str
    options: List[str]
    correct_answer: int  # Index of correct option
    skill: str
    difficulty: str
    explanation: str

class SAQuestion(BaseModel):
    id: int
    question: str
    sample_answer: str
    skill: str
    difficulty: str

class CaseStudy(BaseModel):
    id: int
    title: str
    scenario: str
    questions: List[str]
    skills: List[str]
    difficulty: str

class AptitudeQuestion(BaseModel):
    id: int
    question: str
    correct_answer: str
    explanation: str

class SkillCoverage(BaseModel):
    skill_name: str
    question_count: int
    coverage_percentage: float
    quality: str

class AssessmentResult(BaseModel):
    assessment_id: str
    role_name: str
    target_skills: List[str]
    difficulty_level: str
    mcqs: List[MCQuestion]
    saqs: List[SAQuestion]
    case_study: CaseStudy
    aptitude_questions: List[AptitudeQuestion]
    skill_coverage: List[SkillCoverage]
    total_questions: int
    estimated_duration_minutes: int
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    is_published: Optional[bool] = False

class UpdateAssessmentRequest(BaseModel):
    assessment_id: str
    role_name: str
    target_skills: List[str]
    difficulty_level: str
    mcqs: List[MCQuestion]
    saqs: List[SAQuestion]
    case_study: Optional[CaseStudy]
    aptitude_questions: List[AptitudeQuestion]
    skill_coverage: List[SkillCoverage]
    total_questions: int
    estimated_duration_minutes: int

class DeployAssessmentRequest(BaseModel):
    assessment_id: str
    course_id: int

class AssessmentListItem(BaseModel):
    assessment_id: str
    role_name: str
    target_skills: List[str]
    difficulty_level: str
    total_questions: int
    estimated_duration_minutes: int
    created_by_email: str
    created_at: str
    updated_at: str
    is_published: bool
    deployed_courses_count: int

# ============================================================================
# AI GENERATION FUNCTIONS (keeping existing logic)
# ============================================================================

def generate_mcqs(role: str, skills: List[str], difficulty: str) -> List[MCQuestion]:
    """Generate 15 multiple choice questions"""
    
    prompt = f"""Generate 15 multiple choice questions for a {role} assessment.

Skills to test: {', '.join(skills)}
Difficulty: {difficulty}

For each question:
1. Create a practical question relevant to {role} work
2. Provide 4 realistic options
3. Mark which option is correct (0, 1, 2, or 3)
4. Tag with one skill from: {skills}
5. Add brief explanation

Return as JSON array with this structure:
[
  {{
    "id": 1,
    "question": "What is the primary benefit of indexing in SQL?",
    "options": ["Faster queries", "Smaller database", "Better security", "Automatic backups"],
    "correct_answer": 0,
    "skill": "SQL",
    "difficulty": "{difficulty}",
    "explanation": "Indexes speed up query performance by creating quick lookup paths."
  }}
]

Make questions {difficulty} level - focus on {'basic concepts' if difficulty == 'easy' else 'practical application' if difficulty == 'medium' else 'advanced scenarios'}.
"""

    try:
        response = client.chat.completions.create(
            model="openai/gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an expert assessment creator. Return only valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=3000
        )
        
        content = response.choices[0].message.content.strip()
        # Clean up JSON if needed
        if content.startswith('```json'):
            content = content.replace('```json', '').replace('```', '').strip()
        
        mcq_data = json.loads(content)
        return [MCQuestion(**q) for q in mcq_data[:15]]  # Ensure max 15 questions
        
    except Exception as e:
        logger.error(f"Error generating MCQs: {str(e)}")
        # Return fallback questions if generation fails
        return [
            MCQuestion(
                id=1,
                question=f"What is a key skill for {role}?",
                options=["Communication", "Technical expertise", "Problem solving", "All of the above"],
                correct_answer=3,
                skill=skills[0] if skills else "General",
                difficulty=difficulty,
                explanation="All listed skills are important for professional success."
            )
        ]

def generate_saqs(role: str, skills: List[str], difficulty: str) -> List[SAQuestion]:
    """Generate 5 short answer questions"""
    
    prompt = f"""Generate 5 short answer questions for a {role} assessment.

Skills to test: {', '.join(skills)}
Difficulty: {difficulty}

Create scenario-based questions that require 2-3 paragraph responses.
Include sample answers showing what a good response looks like.

Return as JSON array:
[
  {{
    "id": 11,
    "question": "Describe how you would optimize a slow-running SQL query in a production environment.",
    "sample_answer": "I would start by analyzing the execution plan to identify bottlenecks...",
    "skill": "SQL",
    "difficulty": "{difficulty}"
  }}
]

Focus on {difficulty} scenarios - {'simple workplace situations' if difficulty == 'easy' else 'complex problem-solving' if difficulty == 'medium' else 'strategic challenges'}.
"""

    try:
        response = client.chat.completions.create(
            model="openai/gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an expert assessment creator. Return only valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=2000
        )
        
        content = response.choices[0].message.content.strip()
        if content.startswith('```json'):
            content = content.replace('```json', '').replace('```', '').strip()
        
        saq_data = json.loads(content)
        return [SAQuestion(**q) for q in saq_data[:5]]
        
    except Exception as e:
        logger.error(f"Error generating SAQs: {str(e)}")
        return [
            SAQuestion(
                id=11,
                question=f"Describe your approach to a typical {role} challenge.",
                sample_answer="I would analyze the problem, gather requirements, and develop a systematic solution.",
                skill=skills[0] if skills else "General",
                difficulty=difficulty
            )
        ]

def generate_case_study(role: str, skills: List[str], difficulty: str) -> CaseStudy:
    """Generate 1 case study with 3 questions"""
    
    prompt = f"""Generate 1 case study for a {role} assessment.

Skills to test: {', '.join(skills)}
Difficulty: {difficulty}

Create a realistic business scenario with:
- Detailed situation description
- 3 analysis questions
- Covers multiple skills: {skills}

Return as JSON:
{{
  "id": 14,
  "title": "E-commerce Platform Performance Issues",
  "scenario": "You're working at an e-commerce company that has been experiencing...",
  "questions": [
    "What data would you analyze to identify the root cause?",
    "How would you prioritize the issues you discover?",
    "What solution would you recommend and why?"
  ],
  "skills": {json.dumps(skills)},
  "difficulty": "{difficulty}"
}}

Make it {difficulty} complexity - {'straightforward with clear solutions' if difficulty == 'easy' else 'moderate complexity requiring analysis' if difficulty == 'medium' else 'complex with multiple valid approaches'}.
"""

    try:
        response = client.chat.completions.create(
            model="openai/gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an expert assessment creator. Return only valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=1500
        )
        
        content = response.choices[0].message.content.strip()
        if content.startswith('```json'):
            content = content.replace('```json', '').replace('```', '').strip()
        
        case_data = json.loads(content)
        return CaseStudy(**case_data)
        
    except Exception as e:
        logger.error(f"Error generating case study: {str(e)}")
        return CaseStudy(
            id=14,
            title=f"{role} Challenge",
            scenario=f"You are working as a {role} and need to solve a complex business problem...",
            questions=["How would you approach this?", "What factors would you consider?", "What would be your recommendation?"],
            skills=skills,
            difficulty=difficulty
        )

def generate_aptitude_questions(role: str) -> List[AptitudeQuestion]:
    """Generate 6-8 aptitude questions"""
    
    prompt = f"""Generate 6-8 aptitude questions for a {role} role.

Focus on logical reasoning and problem-solving skills relevant to {role}.
Avoid technical knowledge - test thinking ability.

Return as JSON array:
[
  {{
    "id": 17,
    "question": "If all analysts use data, and some data users make decisions, can we conclude that some analysts make decisions?",
    "correct_answer": "No, this conclusion cannot be drawn from the given premises.",
    "explanation": "This is a logical fallacy. The premises don't establish that analysts are decision makers."
  }}
]

Include patterns, logic puzzles, and reasoning scenarios.
"""

    try:
        response = client.chat.completions.create(
            model="openai/gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an expert assessment creator. Return only valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=2000
        )
        
        content = response.choices[0].message.content.strip()
        if content.startswith('```json'):
            content = content.replace('```json', '').replace('```', '').strip()
        
        apt_data = json.loads(content)
        return [AptitudeQuestion(**q) for q in apt_data[:5]]
        
    except Exception as e:
        logger.error(f"Error generating aptitude questions: {str(e)}")
        return [
            AptitudeQuestion(
                id=17,
                question="What comes next in the sequence: 2, 4, 8, 16, ?",
                correct_answer="32",
                explanation="Each number doubles the previous number."
            )
        ]

def calculate_skill_coverage(mcqs: List[MCQuestion], saqs: List[SAQuestion], 
                           case_study: CaseStudy, target_skills: List[str]) -> List[SkillCoverage]:
    """Calculate skill coverage analysis"""
    
    total_questions = len(mcqs) + len(saqs) + len(case_study.questions)
    skill_counts = {skill: 0 for skill in target_skills}
    
    # Count MCQs
    for mcq in mcqs:
        if mcq.skill in skill_counts:
            skill_counts[mcq.skill] += 1
    
    # Count SAQs
    for saq in saqs:
        if saq.skill in skill_counts:
            skill_counts[saq.skill] += 1
    
    # Count case study (distribute across all skills)
    case_questions = len(case_study.questions)
    for skill in case_study.skills:
        if skill in skill_counts:
            skill_counts[skill] += case_questions / len(case_study.skills)
    
    # Generate coverage analysis
    coverage = []
    for skill in target_skills:
        count = skill_counts[skill]
        percentage = (count / total_questions) * 100 if total_questions > 0 else 0
        
        if percentage >= 25:
            quality = "excellent"
        elif percentage >= 15:
            quality = "good"
        elif percentage >= 8:
            quality = "adequate"
        else:
            quality = "insufficient"
        
        coverage.append(SkillCoverage(
            skill_name=skill,
            question_count=int(count),
            coverage_percentage=round(percentage, 1),
            quality=quality
        ))
    
    return coverage

# ============================================================================
# API ENDPOINTS
# ============================================================================

@router.post("/generate", response_model=AssessmentResult)
async def generate_role_assessment(
    request: GenerateAssessmentRequest
) -> AssessmentResult:
    """
    Generate a complete role-based assessment.
    Returns: 15 MCQs + 5 SAQs + 1 Case Study + 5 Aptitude Questions
    """
    
    current_user = await get_default_user()
    
    try:
        assessment_id = f"role_assessment_{uuid.uuid4().hex[:8]}"
        
        logger.info(f"Generating assessment for role: {request.role}, skills: {request.skills}")
        
        # Generate all question types
        logger.info("Generating MCQ questions...")
        mcqs = generate_mcqs(request.role, request.skills, request.difficulty)
        
        logger.info("Generating SAQ questions...")
        saqs = generate_saqs(request.role, request.skills, request.difficulty)
        
        logger.info("Generating case study...")
        case_study = generate_case_study(request.role, request.skills, request.difficulty)
        
        logger.info("Generating aptitude questions...")
        aptitude_questions = generate_aptitude_questions(request.role)
        
        # Calculate coverage
        logger.info("Calculating skill coverage...")
        skill_coverage = calculate_skill_coverage(mcqs, saqs, case_study, request.skills)
        
        # Assemble final result
        total_questions = len(mcqs) + len(saqs) + len(case_study.questions) + len(aptitude_questions)
        
        result = AssessmentResult(
            assessment_id=assessment_id,
            role_name=request.role,
            target_skills=request.skills,
            difficulty_level=request.difficulty,
            mcqs=mcqs,
            saqs=saqs,
            case_study=case_study,
            aptitude_questions=aptitude_questions,
            skill_coverage=skill_coverage,
            total_questions=total_questions,
            estimated_duration_minutes=60  # 1 hour estimate
        )
        
        # Save to database
        await save_assessment(result.dict(), current_user["org_id"], current_user["id"])
        
        logger.info(f"Assessment generated successfully: {assessment_id}")
        logger.info(f"Generated: {len(mcqs)} MCQs, {len(saqs)} SAQs, 1 Case Study, {len(aptitude_questions)} Aptitude")
        
        return result
        
    except Exception as e:
        logger.error(f"Error generating assessment: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to generate assessment: {str(e)}")

@router.put("/update", response_model=AssessmentResult)
async def update_role_assessment(
    request: UpdateAssessmentRequest
) -> AssessmentResult:
    """Update an existing assessment"""
    
    current_user = await get_default_user()
    
    try:
        # Save updated assessment
        await save_assessment(request.dict(), current_user["org_id"], current_user["id"])
        
        # Fetch and return updated assessment
        updated = await get_assessment(request.assessment_id)
        if not updated:
            raise HTTPException(status_code=404, detail="Assessment not found after update")
        
        return AssessmentResult(**updated)
        
    except Exception as e:
        logger.error(f"Error updating assessment: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to update assessment: {str(e)}")

@router.get("/list/{user_id}", response_model=List[AssessmentListItem])
async def list_role_assessments(user_id: str) -> List[AssessmentListItem]:
    """List all assessments for the organization"""
    
    current_user = await get_default_user(user_id)
    if current_user["role"] not in ["owner", "mentor", "admin"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have permission to perform this action."
        )
    
    try:
        assessments = await list_assessments(current_user["org_id"], current_user["id"])
        return [AssessmentListItem(**a) for a in assessments]
        
    except Exception as e:
        logger.error(f"Error listing assessments: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to list assessments: {str(e)}")

@router.get("/{assessment_id}", response_model=AssessmentResult)
async def get_role_assessment(
    assessment_id: str
) -> AssessmentResult:
    """Get a specific assessment by ID"""
    
    current_user = await get_default_user()
    
    try:
        assessment = await get_assessment(assessment_id)
        if not assessment:
            raise HTTPException(status_code=404, detail="Assessment not found")
        
        return AssessmentResult(**assessment)
        
    except Exception as e:
        logger.error(f"Error getting assessment: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get assessment: {str(e)}")

@router.post("/deploy")
async def deploy_assessment(
    request: DeployAssessmentRequest
):
    """Deploy an assessment to a course"""
    
    current_user = await get_default_user()
    
    try:
        success = await deploy_assessment_to_course(
            request.assessment_id, 
            request.course_id, 
            current_user["id"]
        )
        
        if not success:
            return {"message": "Assessment already deployed to this course"}
        
        return {"message": "Assessment deployed successfully"}
        
    except Exception as e:
        logger.error(f"Error deploying assessment: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to deploy assessment: {str(e)}")

@router.post("/undeploy")
async def undeploy_assessment(
    request: DeployAssessmentRequest
):
    """Undeploy an assessment from a course"""
    
    current_user = await get_default_user()
    
    try:
        await undeploy_assessment_from_course(request.assessment_id, request.course_id)
        return {"message": "Assessment undeployed successfully"}
        
    except Exception as e:
        logger.error(f"Error undeploying assessment: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to undeploy assessment: {str(e)}")

@router.get("/course/{course_id}/assessments")
async def get_course_role_assessments(
    course_id: int
):
    """Get all assessments deployed to a course"""
    
    current_user = await get_default_user()
    
    try:
        assessments = await get_course_assessments(course_id)
        return assessments
        
    except Exception as e:
        logger.error(f"Error getting course assessments: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get course assessments: {str(e)}")

@router.get("/mentor/courses")
async def get_mentor_available_courses():
    """Get courses available for mentor to deploy assessments"""
    
    current_user = await get_default_user()
    
    try:
        courses = await get_mentor_courses(current_user["id"], current_user["org_id"])
        return courses
        
    except Exception as e:
        logger.error(f"Error getting mentor courses: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get mentor courses: {str(e)}")

# ============================================================================
# SIMPLE STATUS ENDPOINT (for frontend compatibility)
# ============================================================================

@router.get("/status/{assessment_id}")
async def get_assessment_status(assessment_id: str):
    """Simple status endpoint - since generation is synchronous, always return completed"""
    return {
        "assessment_id": assessment_id,
        "status": "completed",
        "progress_percentage": 100,
        "current_step": "Assessment completed",
        "estimated_completion_seconds": 0
    }