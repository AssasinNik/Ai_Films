// =====================================================
// MongoDB Setup Script for Movie Recommendation System
// Based on Emotional Mood Analysis
// =====================================================

// Connect to the database (mongosh syntax)
use('movie_recommendation_db');

// Idempotent helpers
function ensureCollection(name, options) {
  const existing = db.getCollectionNames().includes(name);
  if (existing) {
    // Update validator if collection already exists
    const cmd = { collMod: name };
    if (options && options.validator) cmd.validator = options.validator;
    if (options && options.validationAction) cmd.validationAction = options.validationAction;
    db.runCommand(cmd);
  } else {
    db.createCollection(name, options || {});
  }
}

// =====================================================
// COLLECTION: users
// =====================================================

// Create users collection with validation schema
ensureCollection("users", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["user_id", "email", "created_at"],
      properties: {
        user_id: {
          bsonType: "string",
          description: "Unique user identifier - required"
        },
        email: {
          bsonType: "string",
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
          description: "Valid email address - required"
        },
        username: {
          bsonType: "string",
          minLength: 3,
          maxLength: 50,
          description: "Username between 3-50 characters"
        },
        demographics: {
          bsonType: "object",
          properties: {
            age_range: {
              enum: ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"],
              description: "Age range for privacy"
            },
            gender: {
              enum: ["male", "female", "other", "prefer_not_to_say"],
              description: "Gender identification"
            },
            location: {
              bsonType: "object",
              properties: {
                country: { bsonType: "string" },
                region: { bsonType: "string" },
                timezone: { bsonType: "string" }
              }
            },
            languages: {
              bsonType: "array",
              items: { bsonType: "string" }
            }
          }
        },
        preferences: {
          bsonType: "object",
          properties: {
            content_language: { bsonType: "string" },
            subtitle_language: { bsonType: "string" },
            adult_content: { bsonType: "bool" },
            violence_tolerance: {
              enum: ["low", "medium", "high"],
              description: "Violence tolerance level"
            },
            preferred_genres: {
              bsonType: "array",
              items: {
                bsonType: "object",
                required: ["genre_id", "weight"],
                properties: {
                  genre_id: { bsonType: "int" },
                  weight: { 
                    bsonType: "double",
                    minimum: 0,
                    maximum: 1
                  }
                }
              }
            },
            disliked_genres: {
              bsonType: "array",
              items: {
                bsonType: "object",
                required: ["genre_id", "weight"],
                properties: {
                  genre_id: { bsonType: "int" },
                  weight: { 
                    bsonType: "double",
                    minimum: 0,
                    maximum: 1
                  }
                }
              }
            },
            max_movie_length: {
              bsonType: "int",
              minimum: 30,
              maximum: 300
            },
            min_rating: {
              bsonType: "double",
              minimum: 0,
              maximum: 10
            }
          }
        },
        created_at: {
          bsonType: "date",
          description: "Account creation timestamp - required"
        },
        updated_at: {
          bsonType: "date",
          description: "Last update timestamp"
        },
        last_active: {
          bsonType: "date",
          description: "Last activity timestamp"
        }
      }
    }
  }
  , validationAction: "warn"
});

// Create indexes for users collection
db.users.createIndex({ "user_id": 1 }, { unique: true });
db.users.createIndex({ "email": 1 }, { unique: true });
db.users.createIndex({ "username": 1 }, { sparse: true });
db.users.createIndex({ "last_active": -1 });
db.users.createIndex({ "demographics.location.country": 1 });
db.users.createIndex({ "preferences.preferred_genres.genre_id": 1 });

// =====================================================
// COLLECTION: emotional_profiles
// =====================================================

ensureCollection("emotional_profiles", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["user_id", "created_at"],
      properties: {
        user_id: {
          bsonType: "string",
          description: "Reference to user - required"
        },
        current_mood: {
          bsonType: "object",
          properties: {
            primary_emotion: {
              enum: ["happy", "sad", "angry", "excited", "calm", "tired", "stressed", "relaxed", "motivated", "bored", "anxious", "content"],
              description: "Primary emotion"
            },
            intensity: {
              bsonType: "double",
              minimum: 0,
              maximum: 1,
              description: "Emotion intensity 0-1"
            },
            secondary_emotions: {
              bsonType: "array",
              items: {
                bsonType: "object",
                required: ["emotion", "intensity"],
                properties: {
                  emotion: { bsonType: "string" },
                  intensity: {
                    bsonType: "double",
                    minimum: 0,
                    maximum: 1
                  }
                }
              }
            },
            energy_level: {
              bsonType: "double",
              minimum: 0,
              maximum: 1,
              description: "Energy level 0-1"
            },
            stress_level: {
              bsonType: "double",
              minimum: 0,
              maximum: 1,
              description: "Stress level 0-1"
            },
            detection_method: {
              enum: ["self_reported", "ai_detected", "context_inferred"],
              description: "How mood was detected"
            },
            confidence: {
              bsonType: "double",
              minimum: 0,
              maximum: 1,
              description: "Confidence in mood detection"
            }
          }
        },
        mood_history: {
          bsonType: "array",
          items: {
            bsonType: "object",
            properties: {
              date: { bsonType: "date" },
              primary_emotion: { bsonType: "string" },
              intensity: { bsonType: "double" },
              energy_level: { bsonType: "double" },
              context: { bsonType: "string" },
              movies_watched: {
                bsonType: "array",
                items: { bsonType: "string" }
              }
            }
          }
        },
        emotional_patterns: {
          bsonType: "object",
          properties: {
            dominant_emotions: {
              bsonType: "array",
              items: {
                bsonType: "object",
                properties: {
                  emotion: { bsonType: "string" },
                  frequency: { bsonType: "double" }
                }
              }
            },
            time_patterns: {
              bsonType: "object",
              properties: {
                morning: {
                  bsonType: "object",
                  properties: {
                    typical_energy: { bsonType: "double" },
                    common_emotions: {
                      bsonType: "array",
                      items: { bsonType: "string" }
                    }
                  }
                },
                afternoon: {
                  bsonType: "object",
                  properties: {
                    typical_energy: { bsonType: "double" },
                    common_emotions: {
                      bsonType: "array",
                      items: { bsonType: "string" }
                    }
                  }
                },
                evening: {
                  bsonType: "object",
                  properties: {
                    typical_energy: { bsonType: "double" },
                    common_emotions: {
                      bsonType: "array",
                      items: { bsonType: "string" }
                    }
                  }
                },
                night: {
                  bsonType: "object",
                  properties: {
                    typical_energy: { bsonType: "double" },
                    common_emotions: {
                      bsonType: "array",
                      items: { bsonType: "string" }
                    }
                  }
                }
              }
            }
          }
        },
        created_at: {
          bsonType: "date",
          description: "Profile creation timestamp - required"
        },
        updated_at: {
          bsonType: "date",
          description: "Last update timestamp"
        }
      }
    }
  }
,
  validationAction: "warn"
});

// Create indexes for emotional_profiles collection
db.emotional_profiles.createIndex({ "user_id": 1 }, { unique: true });
db.emotional_profiles.createIndex({ "current_mood.primary_emotion": 1 });
db.emotional_profiles.createIndex({ "current_mood.energy_level": 1 });
db.emotional_profiles.createIndex({ "updated_at": -1 });
db.emotional_profiles.createIndex({ "mood_history.date": -1 });

// =====================================================
// COLLECTION: viewing_history
// =====================================================

ensureCollection("viewing_history", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["user_id", "movie_id", "viewing_info", "created_at"],
      properties: {
        user_id: {
          bsonType: "string",
          description: "Reference to user - required"
        },
        movie_id: {
          bsonType: "string",
          description: "Reference to movie - required"
        },
        session_id: {
          bsonType: "string",
          description: "Unique session identifier"
        },
        viewing_info: {
          bsonType: "object",
          required: ["start_time", "duration_watched", "completion_percentage"],
          properties: {
            start_time: { bsonType: "date" },
            end_time: { bsonType: "date" },
            duration_watched: {
              bsonType: "int",
              minimum: 0,
              description: "Minutes actually watched"
            },
            completion_percentage: {
              bsonType: "int",
              minimum: 0,
              maximum: 100,
              description: "Percentage of movie watched"
            },
            watch_method: {
              enum: ["streaming", "download", "tv", "cinema"],
              description: "How the movie was watched"
            },
            device_type: {
              enum: ["mobile", "tablet", "desktop", "smart_tv", "cinema"],
              description: "Device used for watching"
            },
            quality: {
              enum: ["480p", "720p", "1080p", "4K"],
              description: "Video quality"
            }
          }
        },
        context: {
          bsonType: "object",
          properties: {
            time_of_day: {
              enum: ["morning", "afternoon", "evening", "night"],
              description: "Time of day when watched"
            },
            day_of_week: {
              enum: ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"],
              description: "Day of week"
            },
            viewing_alone: {
              bsonType: "bool",
              description: "Whether watched alone"
            },
            planned_viewing: {
              bsonType: "bool",
              description: "Whether viewing was planned"
            },
            recommendation_source: {
              enum: ["mood_based", "trending", "friend", "search", "random"],
              description: "Source of recommendation"
            },
            user_mood_before: {
              bsonType: "object",
              properties: {
                primary_emotion: { bsonType: "string" },
                energy_level: { bsonType: "double" }
              }
            },
            user_mood_after: {
              bsonType: "object",
              properties: {
                primary_emotion: { bsonType: "string" },
                energy_level: { bsonType: "double" }
              }
            }
          }
        },
        user_interaction: {
          bsonType: "object",
          properties: {
            rating_given: {
              bsonType: "int",
              minimum: 1,
              maximum: 10,
              description: "User rating 1-10"
            },
            rating_timestamp: { bsonType: "date" },
            review_written: { bsonType: "bool" },
            shared_socially: { bsonType: "bool" },
            added_to_favorites: { bsonType: "bool" },
            rewatched: { bsonType: "bool" },
            paused_count: { bsonType: "int" },
            skipped_scenes: { bsonType: "bool" }
          }
        },
        emotional_response: {
          bsonType: "object",
          properties: {
            overall_satisfaction: {
              bsonType: "double",
              minimum: 0,
              maximum: 1,
              description: "Overall satisfaction 0-1"
            },
            emotional_impact: {
              enum: ["positive", "negative", "neutral", "mixed"],
              description: "Emotional impact of the movie"
            },
            mood_change: {
              bsonType: "double",
              minimum: -1,
              maximum: 1,
              description: "Mood change -1 to 1"
            },
            would_recommend: { bsonType: "bool" },
            mood_match: {
              bsonType: "double",
              minimum: 0,
              maximum: 1,
              description: "How well movie matched mood"
            }
          }
        },
        created_at: {
          bsonType: "date",
          description: "Record creation timestamp - required"
        }
      }
    }
  }
,
  validationAction: "warn"
});

// Create indexes for viewing_history collection
db.viewing_history.createIndex({ "user_id": 1, "viewing_info.start_time": -1 });
db.viewing_history.createIndex({ "movie_id": 1 });
db.viewing_history.createIndex({ "context.recommendation_source": 1 });
db.viewing_history.createIndex({ "user_interaction.rating_given": 1 });
db.viewing_history.createIndex({ "emotional_response.mood_match": -1 });
db.viewing_history.createIndex({ "context.time_of_day": 1 });
db.viewing_history.createIndex({ "viewing_info.completion_percentage": -1 });

// =====================================================
// COLLECTION: user_recommendations
// =====================================================

ensureCollection("user_recommendations", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["user_id", "generated_at", "recommendation_type"],
      properties: {
        user_id: {
          bsonType: "string",
          description: "Reference to user - required"
        },
        generated_at: {
          bsonType: "date",
          description: "When recommendations were generated - required"
        },
        expires_at: {
          bsonType: "date",
          description: "When recommendations expire"
        },
        recommendation_type: {
          enum: ["mood_based", "collaborative", "content_based", "trending", "hybrid"],
          description: "Type of recommendation algorithm used"
        },
        context: {
          bsonType: "object",
          properties: {
            user_mood: {
              bsonType: "object",
              properties: {
                primary_emotion: { bsonType: "string" },
                energy_level: { bsonType: "double" }
              }
            },
            time_context: { bsonType: "string" },
            available_time: {
              bsonType: "int",
              minimum: 0,
              description: "Available time in minutes"
            },
            viewing_companions: {
              enum: ["alone", "partner", "family", "friends"],
              description: "Who user is watching with"
            }
          }
        },
        recommendations: {
          bsonType: "array",
          items: {
            bsonType: "object",
            required: ["movie_id", "rank", "confidence_score"],
            properties: {
              movie_id: { bsonType: "string" },
              rank: {
                bsonType: "int",
                minimum: 1,
                description: "Recommendation rank"
              },
              confidence_score: {
                bsonType: "double",
                minimum: 0,
                maximum: 1,
                description: "Confidence in recommendation"
              },
              mood_match_score: {
                bsonType: "double",
                minimum: 0,
                maximum: 1,
                description: "How well movie matches mood"
              },
              explanation: {
                bsonType: "object",
                properties: {
                  primary_reason: { bsonType: "string" },
                  secondary_reasons: {
                    bsonType: "array",
                    items: { bsonType: "string" }
                  },
                  mood_explanation: { bsonType: "string" }
                }
              },
              predicted_rating: {
                bsonType: "double",
                minimum: 0,
                maximum: 10,
                description: "Predicted user rating"
              },
              predicted_satisfaction: {
                bsonType: "double",
                minimum: 0,
                maximum: 1,
                description: "Predicted satisfaction"
              }
            }
          }
        },
        quality_metrics: {
          bsonType: "object",
          properties: {
            diversity_score: {
              bsonType: "double",
              minimum: 0,
              maximum: 1,
              description: "Diversity of recommendations"
            },
            novelty_score: {
              bsonType: "double",
              minimum: 0,
              maximum: 1,
              description: "Novelty for user"
            },
            coverage_score: {
              bsonType: "double",
              minimum: 0,
              maximum: 1,
              description: "Coverage of user preferences"
            },
            mood_alignment: {
              bsonType: "double",
              minimum: 0,
              maximum: 1,
              description: "Alignment with user mood"
            }
          }
        },
        feedback: {
          bsonType: "object",
          properties: {
            viewed_recommendations: {
              bsonType: "array",
              items: { bsonType: "string" }
            },
            clicked_recommendations: {
              bsonType: "array",
              items: { bsonType: "string" }
            },
            accepted_recommendations: {
              bsonType: "array",
              items: { bsonType: "string" }
            },
            rejected_recommendations: {
              bsonType: "array",
              items: { bsonType: "string" }
            },
            feedback_timestamp: { bsonType: "date" }
          }
        }
      }
    }
  }
  , validationAction: "warn"
});

// Create indexes for user_recommendations collection
db.user_recommendations.createIndex({ "user_id": 1, "generated_at": -1 });
db.user_recommendations.createIndex({ "expires_at": 1 }, { expireAfterSeconds: 0 });
db.user_recommendations.createIndex({ "recommendation_type": 1 });
db.user_recommendations.createIndex({ "recommendations.movie_id": 1 });
db.user_recommendations.createIndex({ "quality_metrics.mood_alignment": -1 });

// =====================================================
// COLLECTION: mood_detection_sessions
// =====================================================

ensureCollection("mood_detection_sessions", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["user_id", "session_start", "detection_method"],
      properties: {
        user_id: {
          bsonType: "string",
          description: "Reference to user - required"
        },
        session_start: {
          bsonType: "date",
          description: "Session start time - required"
        },
        session_end: {
          bsonType: "date",
          description: "Session end time"
        },
        detection_method: {
          enum: ["questionnaire", "behavioral", "contextual", "ai_analysis", "hybrid"],
          description: "Method used for mood detection - required"
        },
        input_data: {
          bsonType: "object",
          properties: {
            questionnaire_responses: {
              bsonType: "object",
              properties: {
                energy_level: {
                  bsonType: "int",
                  minimum: 1,
                  maximum: 10
                },
                stress_level: {
                  bsonType: "int",
                  minimum: 1,
                  maximum: 10
                },
                social_preference: {
                  enum: ["alone", "with_others"],
                  description: "Social viewing preference"
                },
                content_preference: {
                  enum: ["light", "serious", "mixed"],
                  description: "Content preference"
                },
                time_available: {
                  bsonType: "int",
                  minimum: 0,
                  description: "Available time in minutes"
                }
              }
            },
            behavioral_signals: {
              bsonType: "object",
              properties: {
                app_usage_pattern: { bsonType: "string" },
                previous_selections: {
                  bsonType: "array",
                  items: { bsonType: "string" }
                },
                time_spent_browsing: {
                  bsonType: "int",
                  minimum: 0,
                  description: "Time spent browsing in seconds"
                }
              }
            },
            contextual_data: {
              bsonType: "object",
              properties: {
                time_of_day: { bsonType: "string" },
                weather: { bsonType: "string" },
                calendar_events: { bsonType: "string" }
              }
            }
          }
        },
        detected_mood: {
          bsonType: "object",
          properties: {
            primary_emotion: { bsonType: "string" },
            confidence: {
              bsonType: "double",
              minimum: 0,
              maximum: 1,
              description: "Confidence in detection"
            },
            secondary_emotions: {
              bsonType: "array",
              items: {
                bsonType: "object",
                properties: {
                  emotion: { bsonType: "string" },
                  intensity: { bsonType: "double" }
                }
              }
            },
            energy_level: {
              bsonType: "double",
              minimum: 0,
              maximum: 1
            },
            recommended_content_types: {
              bsonType: "array",
              items: { bsonType: "string" }
            }
          }
        },
        validation: {
          bsonType: "object",
          properties: {
            user_confirmed: { bsonType: "bool" },
            user_correction: { bsonType: "string" },
            accuracy_score: {
              bsonType: "double",
              minimum: 0,
              maximum: 1
            }
          }
        }
      }
    }
  }
  , validationAction: "warn"
});

// Create indexes for mood_detection_sessions collection
db.mood_detection_sessions.createIndex({ "user_id": 1, "session_start": -1 });
db.mood_detection_sessions.createIndex({ "detection_method": 1 });
db.mood_detection_sessions.createIndex({ "detected_mood.primary_emotion": 1 });
db.mood_detection_sessions.createIndex({ "validation.accuracy_score": -1 });

// =====================================================
// COLLECTION: user_feedback
// =====================================================

ensureCollection("user_feedback", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["user_id", "feedback_type", "created_at"],
      properties: {
        user_id: {
          bsonType: "string",
          description: "Reference to user - required"
        },
        feedback_type: {
          enum: ["recommendation_rating", "mood_detection_accuracy", "system_feedback", "bug_report"],
          description: "Type of feedback - required"
        },
        target_id: {
          bsonType: "string",
          description: "ID of target (movie, recommendation, etc.)"
        },
        rating: {
          bsonType: "int",
          minimum: 1,
          maximum: 5,
          description: "Rating 1-5"
        },
        comment: {
          bsonType: "string",
          maxLength: 1000,
          description: "User comment"
        },
        metadata: {
          bsonType: "object",
          description: "Additional metadata specific to feedback type"
        },
        created_at: {
          bsonType: "date",
          description: "Feedback creation timestamp - required"
        }
      }
    }
  }
  , validationAction: "warn"
});

// Create indexes for user_feedback collection
db.user_feedback.createIndex({ "user_id": 1, "created_at": -1 });
db.user_feedback.createIndex({ "feedback_type": 1 });
db.user_feedback.createIndex({ "target_id": 1 });
db.user_feedback.createIndex({ "rating": 1 });

// =====================================================
// SAMPLE DATA INSERTION
// =====================================================

// Insert sample user
db.users.insertOne({
  user_id: "user_sample_001",
  email: "sample@example.com",
  username: "sample_user",
  demographics: {
    age_range: "25-34",
    gender: "prefer_not_to_say",
    location: {
      country: "RU",
      region: "Moscow",
      timezone: "Europe/Moscow"
    },
    languages: ["ru", "en"]
  },
  preferences: {
    content_language: "ru",
    subtitle_language: "ru",
    adult_content: false,
    violence_tolerance: "medium",
    preferred_genres: [
      { genre_id: 1, weight: 0.8 },
      { genre_id: 2, weight: 0.6 }
    ],
    max_movie_length: 180,
    min_rating: 6.0
  },
  settings: {
    notifications: {
      email_recommendations: true,
      push_notifications: false,
      weekly_digest: true
    },
    privacy: {
      profile_visibility: "private",
      share_viewing_history: false,
      allow_mood_tracking: true
    }
  },
  stats: {
    total_movies_watched: 0,
    total_watch_time_minutes: 0,
    average_rating_given: 0,
    recommendations_accepted: 0,
    recommendations_rejected: 0
  },
  created_at: new Date(),
  updated_at: new Date(),
  last_active: new Date()
});

// Insert sample emotional profile
db.emotional_profiles.insertOne({
  user_id: "user_sample_001",
  current_mood: {
    primary_emotion: "happy",
    intensity: 0.7,
    secondary_emotions: [
      { emotion: "excited", intensity: 0.5 }
    ],
    energy_level: 0.6,
    stress_level: 0.2,
    timestamp: new Date(),
    detection_method: "self_reported",
    confidence: 0.85
  },
  mood_history: [],
  emotional_patterns: {
    dominant_emotions: [
      { emotion: "happy", frequency: 0.35 },
      { emotion: "relaxed", frequency: 0.25 }
    ],
    time_patterns: {
      morning: { typical_energy: 0.7, common_emotions: ["motivated", "calm"] },
      afternoon: { typical_energy: 0.6, common_emotions: ["focused", "neutral"] },
      evening: { typical_energy: 0.4, common_emotions: ["tired", "relaxed"] },
      night: { typical_energy: 0.3, common_emotions: ["sleepy", "contemplative"] }
    }
  },
  mood_preferences: {
    happy: {
      preferred_genres: ["комедия", "приключения", "мюзикл"],
      preferred_tone: "upbeat",
      avoid_genres: ["ужасы", "драма"]
    },
    tired: {
      preferred_genres: ["комедия", "семейный"],
      max_length: 90,
      familiar_content: true
    }
  },
  created_at: new Date(),
  updated_at: new Date()
});

// =====================================================
// UTILITY FUNCTIONS
// =====================================================

// Function to clean up expired recommendations
function cleanupExpiredRecommendations() {
  var result = db.user_recommendations.deleteMany({
    expires_at: { $lt: new Date() }
  });
  print("Deleted " + result.deletedCount + " expired recommendations");
}

// Function to get user statistics
function getUserStats(userId) {
  return db.viewing_history.aggregate([
    { $match: { user_id: userId } },
    {
      $group: {
        _id: "$user_id",
        total_movies: { $sum: 1 },
        total_watch_time: { $sum: "$viewing_info.duration_watched" },
        avg_rating: { $avg: "$user_interaction.rating_given" },
        avg_completion: { $avg: "$viewing_info.completion_percentage" }
      }
    }
  ]).toArray();
}

// Function to get mood patterns for a user
function getUserMoodPatterns(userId) {
  return db.viewing_history.aggregate([
    { $match: { user_id: userId } },
    {
      $group: {
        _id: "$context.user_mood_before.primary_emotion",
        count: { $sum: 1 },
        avg_satisfaction: { $avg: "$emotional_response.overall_satisfaction" },
        avg_mood_match: { $avg: "$emotional_response.mood_match" }
      }
    },
    { $sort: { count: -1 } }
  ]).toArray();
}

print("MongoDB setup completed successfully!");
print("Collections created: users, emotional_profiles, viewing_history, user_recommendations, mood_detection_sessions, user_feedback");
print("Indexes created for optimal query performance");
print("Sample data inserted for testing");
print("Utility functions available: cleanupExpiredRecommendations(), getUserStats(userId), getUserMoodPatterns(userId)");

