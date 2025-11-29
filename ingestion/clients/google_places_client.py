"""Google Places API client for fetching reviews and ratings."""

import os
import requests
from typing import Dict, Optional, List
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

GOOGLE_PLACES_API_BASE_URL = "https://places.googleapis.com/v1"


class GooglePlacesClient:
    """Client for Google Places API (New)."""

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Google Places API client.
        
        Args:
            api_key: Google API key. If not provided, will try to get from GOOGLE_API_KEY env var.
        """
        self.api_key = api_key or os.getenv("GOOGLE_API_KEY")
        if not self.api_key:
            raise RuntimeError(
                "Google API key is required. Set GOOGLE_API_KEY environment variable."
            )
        # Verify API key is not empty
        if not self.api_key.strip():
            raise RuntimeError(
                "GOOGLE_API_KEY environment variable is set but empty."
            )

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with API key in header (not query param)."""
        return {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": "id,displayName,rating,userRatingCount,reviews,photos",
        }

    def get_place_details(self, place_id: str) -> Optional[Dict]:
        """
        Get place details including rating and review count.
        
        Args:
            place_id: Google Place ID
            
        Returns:
            Dict with place details or None if not found
        """
        url = f"{GOOGLE_PLACES_API_BASE_URL}/places/{place_id}"
        
        try:
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 403:
                error_detail = response.text
                print(f"[GOOGLE PLACES] 403 Forbidden error for {place_id}")
                print(f"[GOOGLE PLACES] This usually means:")
                print(f"  1. The API key is invalid or not set correctly")
                print(f"  2. The Places API (New) is not enabled for this API key")
                print(f"  3. The API key has restrictions that block this request")
                print(f"[GOOGLE PLACES] Error details: {error_detail[:200]}")
            else:
                print(f"[GOOGLE PLACES] HTTP {response.status_code} error fetching place details for {place_id}: {e}")
                print(f"[GOOGLE PLACES] Response: {response.text[:200]}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"[GOOGLE PLACES] Error fetching place details for {place_id}: {e}")
            return None

    def get_place_reviews(self, place_id: str) -> List[Dict]:
        """
        Get reviews for a place.
        
        Args:
            place_id: Google Place ID
            
        Returns:
            List of review dictionaries
        """
        place_details = self.get_place_details(place_id)
        if not place_details:
            return []
        
        reviews = place_details.get("reviews", [])
        return reviews

    def get_place_rating_info(self, place_id: str) -> Optional[Dict]:
        """
        Get rating and review count for a place.
        
        Args:
            place_id: Google Place ID
            
        Returns:
            Dict with 'rating' and 'user_rating_count' or None
        """
        place_details = self.get_place_details(place_id)
        if not place_details:
            return None
        
        return {
            "rating": place_details.get("rating"),
            "user_rating_count": place_details.get("userRatingCount"),
        }

    def get_place_photo_name(self, place_id: str) -> Optional[str]:
        """
        Get the photo name/reference for a place.
        
        The photo name can be used to construct a photo URL later.
        The URL requires an API key in the header, so we store just the name.
        
        Args:
            place_id: Google Place ID
            
        Returns:
            Photo name string (e.g., "places/ChIJ.../photos/AWn5...") or None if no photo available
        """
        place_details = self.get_place_details(place_id)
        if not place_details:
            return None
        
        photos = place_details.get("photos", [])
        if not photos:
            return None
        
        # Get the first photo (usually the primary/main photo)
        photo = photos[0]
        photo_name = photo.get("name")
        
        return photo_name
    
    def get_place_photo_url(self, photo_name: str, max_width: int = 800) -> str:
        """
        Construct a photo URL from a photo name.
        
        Note: This URL requires the X-Goog-Api-Key header when fetching.
        It cannot be used directly in a browser or <img> tag.
        
        Args:
            photo_name: Photo name from get_place_photo_name()
            max_width: Maximum width for the photo (default 800)
            
        Returns:
            Photo URL string
        """
        return f"{GOOGLE_PLACES_API_BASE_URL}/{photo_name}/media?maxWidthPx={max_width}"

