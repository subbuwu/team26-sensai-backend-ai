# src/api/routes/role_assessment.py
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException
from typing import List, Dict, Optional, Literal
from pydantic import BaseModel, Field
import json
import uuid
import openai
import os
from api.utils.logging import logger

load_dotenv()
router = APIRouter()

# Initialize OpenAI client with custom configuration
api_key = os.getenv("OPENAI_API_KEY")
client = openai.OpenAI( 
    api_key = api_key,
    base_url="https://agent.dev.hyperverge.org"
)

# ============================================================================
# SIMPLIFIED PYDANTIC MODELS
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

# ============================================================================
# AI GENERATION FUNCTIONS
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
# MAIN GENERATE ENDPOINT - SIMPLIFIED
# ============================================================================

@router.post("/generate", response_model=AssessmentResult)
async def generate_role_assessment(request: GenerateAssessmentRequest) -> AssessmentResult:
    """
    Generate a complete role-based assessment synchronously.
    Returns: 15 MCQs + 5 SAQs + 1 Case Study + 5 Aptitude Questions
    """
    
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
        
        logger.info(f"Assessment generated successfully: {assessment_id}")
        logger.info(f"Generated: {len(mcqs)} MCQs, {len(saqs)} SAQs, 1 Case Study, {len(aptitude_questions)} Aptitude")
        
        return result
        
    except Exception as e:
        logger.error(f"Error generating assessment: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to generate assessment: {str(e)}")

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