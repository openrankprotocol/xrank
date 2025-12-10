#!/usr/bin/env python3
import os
import psycopg2
import json
import time
import logging
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from openai import OpenAI

logger = logging.getLogger(__name__)

model_name = "gpt-4.1-mini"

summarization_instructions = """
You are an expert analyst with a warm tone. You specialize in extracting high-signal insights from recent forum-style content, where ideas can be messy, fast-moving, and conversational. Your goal is to distill the essential themes into a summary.

Given the set of messages, produce a JSON object containing:

1. "topic"
   → A concise thematic synthesis (maximum 3 sentences) written in a professional but easily digestible and simple form.
   → The summary should begin with a short introductory phrase in the style of:
       "Top recent conversations focused on...", 
       "In recent chats, people have been talking about...", 
       "Recent discussions are circling around...", etc.
     (Use a natural variant rather than repeating the same phrase.)
   → Focus entirely on the ideas:
        - key motivations, questions, or pain points
        - notable nuances, tradeoffs, or emerging themes
        - relationships or tensions between subtopics
   → Keep it approachable but still thoughtful and insight-rich.

2. "few_words"
   → 3–7 punchy keywords or short phrases that capture the core ideas.
   → Avoid generic filler unless essential.

3. "one_sentence"
   → One friendly, clear sentence that expresses the core insight.

Requirements:
- Maintain a warm, casual, friendly tone while keeping the ideas sharp and useful.
- Capture not just what is being explored, but why it matters and what tensions or possibilities exist.
- Include references to people, projects, or mechanisms only when relevant to the ideas themselves.
- Write cleanly and simply; aim for clarity and usefulness over formality.

Return only the JSON object, nothing else.
"""

def fetch_all_community_ids(db_url: str) -> List[int]:
    """
    Fetch all distinct community_ids we have interactions for.
    Mirrors fetch_all_channel_ids from the first script.
    """
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT community_id FROM xrank.interactions;")
            rows = cur.fetchall()
            return [r[0] for r in rows]
    finally:
        conn.close()


def get_top_posts(
    db_url: str,
    community_id: int,
    limit: int,
) -> List[tuple]:
    """
    Fetch top posts for a single community.

    The logic mirrors the "latest_messages" pattern from the channel script:
      1. First select the last 1000 actual posts (interaction_type='post')
      2. Compute interactions and weighted scores only within that subset
      3. Rank those posts and return the top {limit}
    """

    logger.debug(
        "Fetching top posts for community_id=%s, limit=%s",
        community_id,
        limit,
    )

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:

            query = """
                
                WITH latest_posts AS (
                    SELECT
                        post_id,
                        community_id,
                        created_at,
                        author_user_id,
                        text
                    FROM xrank.interactions
                    WHERE community_id = %s
                      AND interaction_type = 'post'
                      AND text IS NOT NULL
                    ORDER BY created_at DESC, post_id DESC
                    LIMIT 1000
                ),

                
                interaction AS (
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
                    JOIN latest_posts lp
                      ON lp.post_id = COALESCE(
                            i.reply_to_post_id,
                            i.retweeted_post_id,
                            i.quoted_post_id,
                            i.post_id
                        )
                     AND lp.community_id = i.community_id
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
                ),

                
                weighted AS (
                    SELECT
                        post_id,
                        SUM(
                            user_score *
                            CASE interaction_type
                                WHEN 'post'    THEN 5
                                WHEN 'reply'   THEN 10
                                WHEN 'comment' THEN 10
                                WHEN 'retweet' THEN 15
                                WHEN 'quote'   THEN 20
                                ELSE 5
                            END
                        ) AS score
                    FROM interaction_scores
                    GROUP BY post_id
                )

                
                SELECT
                    lp.post_id,
                    lp.community_id,
                    lp.created_at,
                    lp.author_user_id,
                    lp.text,
                    COALESCE(w.score, 0) AS score
                FROM latest_posts lp
                LEFT JOIN weighted w
                  ON w.post_id = lp.post_id
                ORDER BY score DESC, lp.created_at DESC
                LIMIT %s
            """

            cur.execute(query, (community_id, limit))
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

    - Filters out empty or very short messages to save tokens / noise.
    - If there is no sufficiently long content, returns None and
      skips the OpenAI call.

    Retries if the response is not valid JSON or if OpenAI call fails.
    """
    logger.debug("Summarizing %d messages with OpenAI", len(messages))

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
                        "name": "community_summary",
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
            rows = get_top_posts(db_url, community_id, limit)
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
    model: str,
) -> None:
    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                for item in results:
                    s = item.get("summary")
                    if not s:
                        logger.info(
                            "Skipping community_id=%s because summary is missing/None",
                            item.get("community"),
                        )
                        continue

                    community_id = str(item["community"])

                    cur.execute(
                        """
                        INSERT INTO xrank.community_summaries (
                            community_id,
                            summary,
                            topic,
                            few_words,
                            one_sentence,
                            error,
                            model
                        )
                        VALUES (
                            %s,
                            %s::jsonb,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                        )
                        ON CONFLICT (community_id) DO UPDATE SET
                            summary      = EXCLUDED.summary,
                            topic        = EXCLUDED.topic,
                            few_words    = EXCLUDED.few_words,
                            one_sentence = EXCLUDED.one_sentence,
                            error        = EXCLUDED.error,
                            model        = EXCLUDED.model,
                            created_at   = NOW();
                        """,
                        (
                            community_id,
                            json.dumps(s, ensure_ascii=False),
                            s.get("topic"),
                            s.get("few_words"),
                            s.get("one_sentence"),
                            s.get("error"),
                            model,
                        ),
                    )
    finally:
        conn.close()



def process_communities_concurrently(
    db_url: str,
    community_ids: List[int],
    limit: int,
    client: OpenAI,
    max_workers: int = 10,
) -> List[Dict[str, Any]]:
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
                res = {"community": community_id, "error": str(e)}
            results.append(res)

    logger.info("Finished processing all communities, uploading to db")
    save_summaries(
        db_url=db_url,
        results=results,
        model=model_name,
    )
    logger.info("Finished uploading to db")
    return results


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL environment variable is required")

    limit = 50
    max_workers = 5

    client = OpenAI()
    community_ids = fetch_all_community_ids(url)
    logger.info(f"Processing summaries for: {community_ids}")

    if community_ids:
        process_communities_concurrently(
            db_url=url,
            community_ids=community_ids,
            limit=limit,
            client=client,
            max_workers=max_workers,
        )


if __name__ == "__main__":
    main()
