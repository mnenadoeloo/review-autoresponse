from pydantic import BaseModel, Field
from typing import List, Optional, Union, Dict, Any
from pydantic import validator

class Review(BaseModel):
    id_review: str = Field(..., description="Unique review identifier")
    id_user: str = Field(..., description="User identifier")
    user_name: str = Field(..., description="User name")
    nm_id: Union[int, str] = Field(..., description="Product identifier")
    review: str = Field(..., description="Review text")
    rating: int = Field(..., ge=1, le=5, description="Review rating")

    @validator('rating')
    def validate_rating(cls, v):
        if not isinstance(v, int) or v < 1 or v > 5:
            return 1
        return v

    @validator('nm_id')
    def validate_nm_id(cls, v):
        if isinstance(v, str):
            try:
                return int(v)
            except (ValueError, TypeError):
                raise ValueError("nm_id must be convertible to integer")
        return v

    @classmethod
    def from_kafka_input(cls, kafka_input: 'KafkaReviewInput') -> Optional['Review']:
        """Convert a KafkaReviewInput to a Review object"""
        review_text = []
        if kafka_input.text:
            review_text.append(kafka_input.text)
        if kafka_input.pros:
            review_text.append(f"Плюсы: {kafka_input.pros}")
        if kafka_input.cons:
            review_text.append(f"Минусы: {kafka_input.cons}")
                
        combined_text = ' '.join(review_text).strip()
        
        if not combined_text:
            return None

        return cls(
            id_review=str(kafka_input.id),
            id_user=str(kafka_input.globalUserId),
            user_name=str(kafka_input.wbUserDetails.name),
            nm_id=int(kafka_input.nmId),
            review=combined_text,
            rating=kafka_input.ProductValuation or 4
        )

class NmIdData(BaseModel):
    title: str = Field(default="Unknown", description="Product title")
    category: str = Field(default="Unknown", description="Product category")

class ProcessedReview(BaseModel):
    review: Review = Field(..., description="Original review data")
    product_data: NmIdData = Field(..., description="Product data")

class GenerationResponse(BaseModel):
    response: str = Field(..., description="Generated response")
    metadata: ProcessedReview = Field(..., description="Review processing metadata")

class ReviewGenerationResponse(BaseModel):
    generations: List[GenerationResponse] = Field(..., description="Generated responses for reviews")

# Keep other existing models unchanged
class Message(BaseModel):
    role: str = Field(..., description="Role of the message sender (system/user/assistant)")
    content: str = Field(..., description="Content of the message")

class ChatPromptRequest(BaseModel):
    messages: List[Message] = Field(..., description="List of messages in the conversation")

class BatchChatRequest(BaseModel):
    conversations: List[ChatPromptRequest] = Field(..., description="List of conversations to process")
    max_tokens: Optional[int] = Field(None, description="Maximum tokens in response")
    temperature: Optional[float] = Field(None, description="Temperature for response generation")

class ChatResponse(BaseModel):
    content: str = Field(..., description="Generated response text")
    conversation_index: int = Field(..., description="Index of the original conversation")

class BatchChatResponse(BaseModel):
    responses: List[ChatResponse] = Field(..., description="List of generated responses")

class WBUserDetails(BaseModel):
    name: str = Field(..., description="User name from Wildberries")

class KafkaReviewInput(BaseModel):
    id: Union[int, str] = Field(..., description="Review ID")
    globalUserId: Union[int, str] = Field(..., description="Global user ID")
    wbUserId: Union[int, str] = Field(..., description="Wildberries user ID")
    imtId: Union[int, str] = Field(..., description="IMT ID")
    nmId: Union[int, str] = Field(..., description="Product ID")
    wbUserDetails: WBUserDetails = Field(..., description="User details")
    text: Optional[str] = Field(None, description="Main review text")
    pros: Optional[str] = Field(None, description="Positive aspects")
    cons: Optional[str] = Field(None, description="Negative aspects")
    ProductValuation: Optional[int] = Field(None, description="Product rating")

class KafkaReviewGenerationRequest(BaseModel):
    reviews: List[KafkaReviewInput] = Field(..., description="List of reviews to process in Kafka message format")

class ReviewGenerationRequest(BaseModel):
    reviews: List[Review] = Field(..., description="List of processed reviews") 