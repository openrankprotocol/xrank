#!/usr/bin/env python3
import os
import psycopg2
import argparse
import json
import time
import logging
from typing import List, Optional, Dict, Any

from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI

logger = logging.getLogger(__name__)

model_name = "gpt-4.1-mini"

summarization_instructions = """
You are an expert analyst specializing in high-signal summarization of long, messy,
and conversational text. Your job is to extract the most important ideas with
maximum clarity and usefulness — **without referencing the existence of a conversation,
participants, speakers, or dialogue**.

Given the set of messages, produce a JSON object containing:

1. "topic"
   → A rich, multi-sentence thematic synthesis written in a direct, content-only style.
   → Do NOT use phrases like “the conversation”, “participants”, “speakers”, “discussion”,
     or anything that implies a transcript.
   → Describe the ideas themselves:
       - central debates and nuanced perspectives,
       - motivations, risks, and implications,
       - relationships between subtopics,
       - underlying tensions, patterns, or insights.
   → This should read like a concise research brief describing the subject matter, not
     a report about a conversation.

2. "few_words"
   → 3–7 ultra-salient keywords or short phrases capturing the core ideas.
   → Avoid generic terminology unless truly central.

3. "one_sentence"
   → One highly informative sentence that synthesizes the entire content.
   → Must NOT reference a conversation or dialogue; instead directly state the core insight.

Requirements:
- Absolutely avoid meta-language such as “this conversation”, “they discuss”, 
  “participants mention”, “the dialogue covers”, etc.
- Be specific, concrete, and insight-driven.
- Capture the “why”, not just the “what”.
- Highlight notable viewpoints, disagreements, or unresolved questions.
- If the content mentions individuals, projects, or mechanisms, include their significance.
- Write for an expert audience; do not oversimplify.

Return only the JSON object, nothing else.
"""



def get_top_posts(
    db_url: str,
    community_id: int,
    run_id: Optional[int],
    limit: int,
) -> List[tuple]:
    """
    Fetch top posts for a single community.

    We:
      - Look at xrank.interactions inside the community
      - Map each interaction to a "post_id" it contributes to
        (post itself, reply target, quoted/retweeted post, etc.)
      - Weight those interactions with the user's score from xrank.scores
      - Rank root posts by this weighted interaction score.

    If run_id is None, do not filter scores by run_id.

    NOTE: We filter out posts whose text is NULL so we only summarize posts
    that actually have content.
    """
    logger.debug(
        "Fetching top posts for community_id=%s, run_id=%s, limit=%s",
        community_id,
        run_id,
        limit,
    )

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            run_id_filter = "AND s.run_id = %s" if run_id is not None else ""

            # NOTE:
            # - We treat every interaction row as contributing to some post_id:
            #   COALESCE(reply_to_post_id, retweeted_post_id, quoted_post_id, post_id)
            # - Then we join with xrank.scores based on (community_id, user_id).
            # - Finally, we join the aggregated scores back to the "post" interactions
            #   (interaction_type = 'post') that we actually want to summarize.
            query = f"""
                WITH interaction AS (
                    SELECT
                        i.community_id,
                        COALESCE(
                            i.reply_to_post_id,
                            i.retweeted_post_id,
                            i.quoted_post_id,
                            i.post_id
                        ) AS post_id,
                        i.author_user_id AS user_id,
                        i.interaction_type
                    FROM xrank.interactions i
                    WHERE i.community_id = %s
                ),
                interaction_scores AS (
                    SELECT
                        i.post_id,
                        i.interaction_type,
                        s.user_id,
                        s.score AS user_score
                    FROM interaction i
                    JOIN xrank.scores s
                      ON s.community_id = i.community_id
                     AND s.user_id = i.user_id
                     {run_id_filter}
                ),
                weighted AS (
                    SELECT
                        post_id,
                        SUM(
                            user_score *
                            CASE interaction_type
                                WHEN 'post' THEN 5
                                WHEN 'reply' THEN 10
                                WHEN 'comment' THEN 10
                                WHEN 'retweet' THEN 15
                                WHEN 'quote' THEN 20
                                ELSE 5
                            END
                        ) AS score
                    FROM interaction_scores
                    GROUP BY post_id
                )
                SELECT
                    p.post_id,
                    p.community_id,
                    p.created_at,
                    p.author_user_id,
                    p.text,
                    COALESCE(w.score, 0) AS score
                FROM xrank.interactions p
                LEFT JOIN weighted w
                  ON w.post_id = p.post_id
                WHERE p.community_id = %s
                  AND p.interaction_type = 'post'
                  AND p.text IS NOT NULL
                ORDER BY score DESC, p.created_at DESC
                LIMIT %s
            """

            if run_id is not None:
                params = (community_id, run_id, community_id, limit)
            else:
                params = (community_id, community_id, limit)

            cur.execute(query, params)
            rows = cur.fetchall()
            logger.debug(
                "Fetched %d posts for community_id=%s", len(rows), community_id
            )
            return rows
    finally:
        conn.close()


def summarize_with_openai(
    messages: List[str],
    client: OpenAI,
    max_retries: int = 3,
    base_delay: float = 1.0,
) -> Dict[str, Any]:
    """
    Summarize messages (post texts) using a shared OpenAI client.

    Improvements:
      - Filters out empty or very short messages to save tokens / noise.
      - If there is no sufficiently long content, returns a "No Content" summary
        and skips the OpenAI call.

    Retries if the response is not valid JSON or if OpenAI call fails.
    """
    logger.debug("Summarizing %d messages with OpenAI", len(messages))

    # Filter out empty or very short messages to save tokens/noise
    valid_messages = [m for m in messages if m and len(m.strip()) > 5]

    if not valid_messages:
        logger.info("No valid messages to summarize; returning None")
        return None

    messages_json = json.dumps(valid_messages, ensure_ascii=False)
    prompt = "Conversation:\n" f"{messages_json}"

    last_error: Optional[Exception] = None

    for attempt in range(max_retries):
        try:
            logger.debug("OpenAI summarize attempt %d/%d", attempt + 1, max_retries)
            resp = client.responses.create(
                model=model_name,
                input=prompt,
                temperature=0.1,
                instructions=summarization_instructions,
                text={
                    "format": {
                        "type": "json_schema",
                        "name": "channel_summary",
                        "schema": {
                            "type": "object",
                            "properties": {
                                "topic": {"type": "string"},
                                "few_words": {"type": "string"},
                                "one_sentence": {"type": "string"},
                            },
                            "required": ["topic", "few_words", "one_sentence"],
                            "additionalProperties": False,
                        },
                        "strict": True,
                    }
                },
            )
            content = resp.output_text.strip()
            logger.debug("OpenAI raw response text: %s", content[:500])
            return json.loads(content)
        except Exception as e:
            last_error = e
            logger.warning(
                "OpenAI summarize failed on attempt %d/%d: %s",
                attempt + 1,
                max_retries,
                e,
            )
            if attempt < max_retries - 1:
                delay = base_delay * (2**attempt)
                logger.debug("Sleeping for %s seconds before retry", delay)
                time.sleep(delay)

    logger.error(
        "OpenAI summarization failed after %d attempts: %s",
        max_retries,
        last_error,
    )
    return {
        "topic": None,
        "few_words": None,
        "one_sentence": None,
        "error": f"Failed to summarize after {max_retries} attempts: {last_error}",
    }


def process_community(
    db_url: str,
    community_id: int,
    run_id: Optional[int],
    limit: int,
    client: OpenAI,
    max_retries: int = 2,
) -> Dict[str, Any]:
    """
    Process a single community: fetch top posts and summarize.
    Retries the whole processing a few times on failure.
    """
    logger.info("Processing community_id=%s", community_id)
    last_error: Optional[Exception] = None

    for attempt in range(max_retries):
        try:
            logger.debug(
                "Community %s processing attempt %d/%d",
                community_id,
                attempt + 1,
                max_retries,
            )
            rows = get_top_posts(db_url, community_id, run_id, limit)
            posts: List[Dict[str, Any]] = []
            for r in rows:
                posts.append(
                    {
                        "post_id": r[0],
                        "community_id": r[1],
                        "created_at": (
                            r[2].isoformat()
                            if hasattr(r[2], "isoformat")
                            else str(r[2])
                        ),
                        "author_user_id": r[3],
                        "message": r[4],
                        "score": r[5],
                    }
                )

            texts_to_summarize = [x["message"] for x in posts if x["message"]]
            summary = summarize_with_openai(texts_to_summarize, client=client)

            logger.info("Finished processing community_id=%s", community_id)
            return {
                "community": community_id,
                "summary": summary,
            }
        except Exception as e:
            last_error = e
            logger.warning(
                "Failed to process community_id=%s on attempt %d/%d: %s",
                community_id,
                attempt + 1,
                max_retries,
                e,
            )
            if attempt < max_retries - 1:
                delay = 1.0 * (2**attempt)
                logger.debug(
                    "Community %s retrying after %s seconds",
                    community_id,
                    delay,
                )
                time.sleep(delay)

    logger.error(
        "Failed to process community_id=%s after %d attempts: %s",
        community_id,
        max_retries,
        last_error,
    )
    return {
        "community": community_id,
        "error": (
            f"Failed to process community after {max_retries} "
            f"attempts: {last_error}"
        ),
    }


def save_summaries(
    db_url: str,
    results: List[Dict[str, Any]],
    run_id: Optional[int],
    limit: int,
    model: str,
) -> None:
    """
    Persist summaries into xrank.community_summaries.

    This assumes a table similar to trank.channel_summaries, e.g.:

        CREATE TABLE xrank.community_summaries (
            community_id BIGINT NOT NULL,
            run_id INTEGER,
            posts_limit INTEGER,
            summary JSONB,
            topic TEXT,
            few_words TEXT,
            one_sentence TEXT,
            error TEXT,
            model TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (community_id, run_id)
        );

    Adjust table/column names here if your schema differs.
    """
    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                for item in results:
                    if "summary" not in item:
                        continue

                    s = item["summary"]
                    if s is None:
                        logger.info(
                            "Skipping community_id=%s because summary is None",
                            item.get("community"),
                        )
                        continue

                    community_id = str(item["community"])
                    topic = s.get("topic")
                    few_words = s.get("few_words")
                    one_sentence = s.get("one_sentence")
                    error = s.get("error")

                    if run_id is None:
                        cur.execute(
                            """
                            INSERT INTO xrank.community_summaries (
                                community_id,
                                run_id,
                                posts_limit,
                                summary,
                                topic,
                                few_words,
                                one_sentence,
                                error,
                                model
                            )
                            VALUES (
                                %s,
                                NULL,
                                %s,
                                %s::jsonb,
                                %s,
                                %s,
                                %s,
                                %s,
                                %s
                            )
                            ON CONFLICT (community_id)
                            WHERE run_id IS NULL DO UPDATE SET
                                posts_limit = EXCLUDED.posts_limit,
                                summary = EXCLUDED.summary,
                                topic = EXCLUDED.topic,
                                few_words = EXCLUDED.few_words,
                                one_sentence = EXCLUDED.one_sentence,
                                error = EXCLUDED.error,
                                model = EXCLUDED.model,
                                created_at = NOW();
                            """,
                            (
                                community_id,
                                limit,
                                json.dumps(s, ensure_ascii=False),
                                topic,
                                few_words,
                                one_sentence,
                                error,
                                model,
                            ),
                        )
                    else:
                        cur.execute(
                            """
                            INSERT INTO xrank.community_summaries (
                                community_id,
                                run_id,
                                posts_limit,
                                summary,
                                topic,
                                few_words,
                                one_sentence,
                                error,
                                model
                            )
                            VALUES (
                                %s,
                                %s,
                                %s,
                                %s::jsonb,
                                %s,
                                %s,
                                %s,
                                %s,
                                %s
                            )
                            ON CONFLICT (community_id, run_id)
                            WHERE run_id IS NOT NULL DO UPDATE SET
                                posts_limit = EXCLUDED.posts_limit,
                                summary = EXCLUDED.summary,
                                topic = EXCLUDED.topic,
                                few_words = EXCLUDED.few_words,
                                one_sentence = EXCLUDED.one_sentence,
                                error = EXCLUDED.error,
                                model = EXCLUDED.model,
                                created_at = NOW();
                            """,
                            (
                                community_id,
                                str(run_id),
                                limit,
                                json.dumps(s, ensure_ascii=False),
                                topic,
                                few_words,
                                one_sentence,
                                error,
                                model,
                            ),
                        )
    finally:
        conn.close()


def process_communities_concurrently(
    db_url: str,
    community_ids: List[int],
    run_id: Optional[int],
    limit: int,
    client: OpenAI,
    max_workers: int = 10,
) -> List[Dict[str, Any]]:
    """
    Process multiple communities concurrently in a thread pool.
    Uses a shared OpenAI client instance across all workers.
    """
    logger.info(
        "Processing %d communities concurrently with max_workers=%d",
        len(community_ids),
        max_workers,
    )
    results: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_community = {
            executor.submit(
                process_community,
                db_url,
                community_id,
                run_id,
                limit,
                client,
            ): community_id
            for community_id in community_ids
        }

        for future in as_completed(future_to_community):
            community_id = future_to_community[future]
            try:
                res = future.result()
            except Exception as e:
                logger.exception(
                    "Unhandled error while processing community_id=%s: %s",
                    community_id,
                    e,
                )
                res = {
                    "community": community_id,
                    "error": str(e),
                }
            results.append(res)

    logger.info("Finished processing all communities, uploading to db")
    save_summaries(
        db_url=db_url,
        results=results,
        run_id=run_id,
        limit=limit,
        model=model_name,
    )
    logger.info("Finished uploading to db")
    return results


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--community-id",
        type=int,
        action="append",
        dest="community_ids",
        required=True,
        help="Community ID (repeat this flag for multiple communities)",
    )
    parser.add_argument("--run-id", type=int, required=False)
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Number of top posts to summarize per community",
    )
    args = parser.parse_args()

    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL environment variable is required")

    client = OpenAI()

    _results = process_communities_concurrently(
        db_url=url,
        community_ids=args.community_ids,
        run_id=args.run_id,
        limit=args.limit,
        client=client,
        max_workers=5,
    )


if __name__ == "__main__":
    main()
