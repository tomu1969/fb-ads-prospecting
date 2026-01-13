"""
Instagram Warm-Up Orchestrator

Main CLI for automating Instagram prospect engagement before cold outreach.
Runs through a 7-day warm-up sequence: follow → like → comment → ready for DM.

Usage:
    # Initialize warm-up for prospects
    python scripts/instagram_warmup/warmup_orchestrator.py --init --csv output/prospects_final.csv

    # Run daily warm-up actions
    python scripts/instagram_warmup/warmup_orchestrator.py --daily --dry-run

    # Check warm-up status
    python scripts/instagram_warmup/warmup_orchestrator.py --status
"""

import os
import sys
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from dotenv import load_dotenv
from tqdm import tqdm

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.instagram_warmup.warmup_tracker import WarmupTracker, WarmupState
from scripts.instagram_warmup.warmup_actions import WarmupActions
from scripts.instagram_warmup.comment_generator import CommentGenerator

load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('instagram_warmup.log')
    ]
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent.parent.parent
CONFIG_DIR = BASE_DIR / 'config'
STATE_FILE = CONFIG_DIR / 'warmup_state.csv'
CONFIG_FILE = CONFIG_DIR / 'warmup_config.json'

# Default configuration
DEFAULT_CONFIG = {
    "phases": {
        "1": {"action": "follow", "description": "Follow the prospect"},
        "2": {"action": "like", "count": 2, "description": "Like 2 recent posts"},
        "3": {"action": "like", "count": 2, "description": "Like 2 more posts"},
        "4": {"action": "comment", "count": 1, "description": "Comment on 1 post"},
        "5": {"action": "like", "count": 1, "description": "Like 1 post"},
        "6": {"action": "comment", "count": 1, "optional": True, "description": "Optional second comment"},
        "7": {"action": "ready", "description": "Ready for DM"}
    },
    "limits": {
        "max_follows_per_day": 25,
        "max_likes_per_day": 75,
        "max_comments_per_day": 12,
        "min_delay_seconds": 30,
        "max_delay_seconds": 120
    }
}


def load_config() -> Dict:
    """Load configuration from file or use defaults."""
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Error loading config: {e}, using defaults")
    return DEFAULT_CONFIG


def save_config(config: Dict):
    """Save configuration to file."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)
    logger.info(f"Config saved to {CONFIG_FILE}")


def init_warmup(csv_path: Path, tracker: WarmupTracker) -> int:
    """Initialize warm-up by importing prospects from CSV."""
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        return 0

    count = tracker.import_from_csv(csv_path)
    logger.info(f"Imported {count} prospects for warm-up")
    return count


def run_daily_warmup(
    tracker: WarmupTracker,
    actions: WarmupActions,
    comment_gen: CommentGenerator,
    config: Dict,
    limit: Optional[int] = None,
    dry_run: bool = False
) -> Dict:
    """
    Run daily warm-up actions for all prospects.

    Returns summary of actions taken.
    """
    stats = {
        'follows': 0,
        'likes': 0,
        'comments': 0,
        'errors': 0,
        'ready_for_dm': 0
    }

    limits = config.get('limits', DEFAULT_CONFIG['limits'])
    phases = config.get('phases', DEFAULT_CONFIG['phases'])

    # Get in-progress prospects
    in_progress = tracker.get_prospects_by_status('in_progress')
    pending = tracker.get_prospects_by_status('pending')

    # Start some pending prospects
    max_new = limits['max_follows_per_day'] - stats['follows']
    for prospect in pending[:max_new]:
        if limit and (stats['follows'] + stats['likes'] + stats['comments']) >= limit:
            break

        # Start warm-up for this prospect
        tracker.advance_phase(prospect.instagram_handle)
        in_progress.append(tracker.prospects[prospect.instagram_handle])

    # Process in-progress prospects
    prospects_to_process = in_progress[:limit] if limit else in_progress

    for prospect in tqdm(prospects_to_process, desc="Processing warm-up", unit="prospect"):
        if limit and (stats['follows'] + stats['likes'] + stats['comments']) >= limit:
            break

        phase = prospect.current_phase
        phase_config = phases.get(str(phase), {})
        action = phase_config.get('action', 'ready')

        logger.info(f"[{prospect.instagram_handle}] Phase {phase}: {phase_config.get('description', action)}")

        try:
            if action == 'follow':
                if stats['follows'] < limits['max_follows_per_day']:
                    if not prospect.followed_at:
                        success, error = actions.follow(prospect.instagram_handle)
                        if success:
                            tracker.mark_followed(prospect.instagram_handle)
                            stats['follows'] += 1
                        else:
                            tracker.set_error(prospect.instagram_handle, error)
                            stats['errors'] += 1
                            continue

                    # Advance to next phase
                    tracker.advance_phase(prospect.instagram_handle)

            elif action == 'like':
                count = phase_config.get('count', 1)
                posts = actions.get_recent_posts(prospect.instagram_handle, limit=10)

                liked = 0
                for post in posts:
                    if liked >= count:
                        break
                    if stats['likes'] >= limits['max_likes_per_day']:
                        break
                    if post['url'] in prospect.posts_liked:
                        continue

                    success, error = actions.like_post(post['url'])
                    if success:
                        tracker.add_liked_post(prospect.instagram_handle, post['url'])
                        stats['likes'] += 1
                        liked += 1
                    else:
                        tracker.set_error(prospect.instagram_handle, error)
                        stats['errors'] += 1

                if liked >= count:
                    tracker.advance_phase(prospect.instagram_handle)

            elif action == 'comment':
                count = phase_config.get('count', 1)
                posts = actions.get_recent_posts(prospect.instagram_handle, limit=10)

                commented = 0
                for post in posts:
                    if commented >= count:
                        break
                    if stats['comments'] >= limits['max_comments_per_day']:
                        break
                    if post['url'] in prospect.posts_commented:
                        continue
                    if post['url'] in prospect.posts_liked:
                        # Prefer posts we haven't interacted with
                        continue

                    # Generate comment
                    comment = comment_gen.generate_comment(
                        page_name=prospect.page_name,
                        post_caption=post.get('caption', ''),
                        post_type='image'
                    )

                    success, error = actions.comment(post['url'], comment)
                    if success:
                        tracker.add_comment(prospect.instagram_handle, post['url'], comment)
                        stats['comments'] += 1
                        commented += 1
                    else:
                        tracker.set_error(prospect.instagram_handle, error)
                        stats['errors'] += 1

                if commented >= count or phase_config.get('optional', False):
                    tracker.advance_phase(prospect.instagram_handle)

            elif action == 'ready':
                tracker.update_status(prospect.instagram_handle, 'ready_for_dm')
                stats['ready_for_dm'] += 1
                logger.info(f"[{prospect.instagram_handle}] Ready for DM!")

        except Exception as e:
            logger.error(f"Error processing {prospect.instagram_handle}: {e}")
            tracker.set_error(prospect.instagram_handle, str(e))
            stats['errors'] += 1

    # Save state
    tracker.save()

    return stats


def generate_manual_checklist(
    tracker: WarmupTracker,
    config: Dict,
    limit: Optional[int] = None,
    output_file: Optional[Path] = None
) -> str:
    """
    Generate a manual checklist for today's warm-up tasks.

    Returns markdown-formatted checklist.
    """
    phases = config.get('phases', DEFAULT_CONFIG['phases'])
    limits = config.get('limits', DEFAULT_CONFIG['limits'])

    lines = []
    lines.append(f"# Instagram Warm-Up Checklist")
    lines.append(f"**Date:** {datetime.now().strftime('%Y-%m-%d')}")
    lines.append("")

    # Get prospects by action type
    to_follow = []
    to_like = []
    to_comment = []
    ready = []

    # Pending prospects to start
    pending = tracker.get_prospects_by_status('pending')
    max_new = min(len(pending), limits['max_follows_per_day'])
    if limit:
        max_new = min(max_new, limit)

    for p in pending[:max_new]:
        to_follow.append(p)

    # In-progress prospects
    in_progress = tracker.get_prospects_by_status('in_progress')

    for prospect in in_progress:
        phase = prospect.current_phase
        phase_config = phases.get(str(phase), {})
        action = phase_config.get('action', 'ready')

        if action == 'follow' and not prospect.followed_at:
            to_follow.append(prospect)
        elif action == 'like':
            count = phase_config.get('count', 1)
            needed = count - len([url for url in [] if url not in prospect.posts_liked])
            if needed > 0:
                to_like.append((prospect, count))
        elif action == 'comment':
            count = phase_config.get('count', 1)
            if prospect.comments_completed < count:
                to_comment.append((prospect, count))
        elif action == 'ready':
            ready.append(prospect)

    # Apply limits
    if limit:
        to_follow = to_follow[:limit]
        to_like = to_like[:limit]
        to_comment = to_comment[:limit]

    # Section: Accounts to Follow
    lines.append("---")
    lines.append(f"## 1. FOLLOW ({len(to_follow)} accounts)")
    lines.append("Open Instagram and follow these accounts:")
    lines.append("")
    if to_follow:
        for p in to_follow:
            lines.append(f"- [ ] **@{p.instagram_handle}** - {p.page_name}")
            lines.append(f"      https://instagram.com/{p.instagram_handle}")
    else:
        lines.append("*No accounts to follow today*")
    lines.append("")

    # Section: Posts to Like
    lines.append("---")
    lines.append(f"## 2. LIKE POSTS ({len(to_like)} accounts)")
    lines.append("Visit each profile and like their recent posts:")
    lines.append("")
    if to_like:
        for p, count in to_like:
            already_liked = len(p.posts_liked)
            lines.append(f"- [ ] **@{p.instagram_handle}** - Like {count} posts (already liked: {already_liked})")
            lines.append(f"      https://instagram.com/{p.instagram_handle}")
    else:
        lines.append("*No posts to like today*")
    lines.append("")

    # Section: Posts to Comment
    lines.append("---")
    lines.append(f"## 3. COMMENT ({len(to_comment)} accounts)")
    lines.append("Leave a genuine, short comment on a recent post:")
    lines.append("")
    lines.append("**Comment tips:**")
    lines.append("- Keep it short (5-15 words)")
    lines.append("- Reference something specific in their post")
    lines.append("- NO sales pitch, NO questions, NO self-promotion")
    lines.append("")
    if to_comment:
        for p, count in to_comment:
            lines.append(f"- [ ] **@{p.instagram_handle}** - Comment on {count} post(s)")
            lines.append(f"      https://instagram.com/{p.instagram_handle}")
            lines.append(f"      Company: {p.page_name}")
    else:
        lines.append("*No comments needed today*")
    lines.append("")

    # Section: Ready for DM
    if ready:
        lines.append("---")
        lines.append(f"## 4. READY FOR DM ({len(ready)} prospects)")
        lines.append("These prospects have completed warm-up and are ready for your message:")
        lines.append("")
        for p in ready:
            lines.append(f"- [ ] **@{p.instagram_handle}** - {p.page_name} ({p.contact_name})")
            lines.append(f"      https://instagram.com/{p.instagram_handle}")
        lines.append("")

    # Summary
    lines.append("---")
    lines.append("## Summary")
    lines.append(f"- Follows: {len(to_follow)}")
    lines.append(f"- Like sessions: {len(to_like)}")
    lines.append(f"- Comments: {len(to_comment)}")
    lines.append(f"- Ready for DM: {len(ready)}")
    lines.append("")
    lines.append("**After completing tasks, run:**")
    lines.append("```")
    lines.append("python scripts/instagram_warmup/warmup_orchestrator.py --mark-done")
    lines.append("```")

    checklist = "\n".join(lines)

    # Save to file if specified
    if output_file:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            f.write(checklist)
        logger.info(f"Checklist saved to {output_file}")

    return checklist


def mark_daily_done(tracker: WarmupTracker, config: Dict):
    """
    Mark today's manual tasks as done and advance phases.

    Call this after completing the manual checklist.
    """
    phases = config.get('phases', DEFAULT_CONFIG['phases'])
    advanced = 0

    # Get all in-progress prospects
    in_progress = tracker.get_prospects_by_status('in_progress')
    pending = tracker.get_prospects_by_status('pending')

    # Start pending prospects (mark as followed)
    for p in pending[:25]:  # Daily limit
        tracker.advance_phase(p.instagram_handle)
        tracker.mark_followed(p.instagram_handle)
        advanced += 1

    # Advance in-progress prospects
    for prospect in in_progress:
        phase = prospect.current_phase
        phase_config = phases.get(str(phase), {})
        action = phase_config.get('action', 'ready')

        if action == 'follow':
            tracker.mark_followed(prospect.instagram_handle)
            tracker.advance_phase(prospect.instagram_handle)
            advanced += 1
        elif action == 'like':
            count = phase_config.get('count', 1)
            # Assume they liked the posts
            for i in range(count):
                tracker.add_liked_post(prospect.instagram_handle, f"manual_like_{datetime.now().isoformat()}_{i}")
            tracker.advance_phase(prospect.instagram_handle)
            advanced += 1
        elif action == 'comment':
            count = phase_config.get('count', 1)
            for i in range(count):
                tracker.add_comment(prospect.instagram_handle, f"manual_comment_{datetime.now().isoformat()}_{i}", "manual comment")
            tracker.advance_phase(prospect.instagram_handle)
            advanced += 1
        elif action == 'ready':
            tracker.update_status(prospect.instagram_handle, 'ready_for_dm')
            advanced += 1

    tracker.save()
    return advanced


def print_status(tracker: WarmupTracker):
    """Print warm-up status summary."""
    pending = tracker.get_prospects_by_status('pending')
    in_progress = tracker.get_prospects_by_status('in_progress')
    ready = tracker.get_prospects_by_status('ready_for_dm')
    dm_sent = tracker.get_prospects_by_status('dm_sent')
    failed = tracker.get_prospects_by_status('failed')

    print("\n" + "=" * 60)
    print("INSTAGRAM WARM-UP STATUS")
    print("=" * 60)
    print(f"Total prospects:    {len(tracker.prospects)}")
    print(f"Pending:            {len(pending)}")
    print(f"In Progress:        {len(in_progress)}")
    print(f"Ready for DM:       {len(ready)}")
    print(f"DM Sent:            {len(dm_sent)}")
    print(f"Failed:             {len(failed)}")
    print("=" * 60)

    # Show phase breakdown for in-progress
    if in_progress:
        print("\nIn Progress by Phase:")
        for phase in range(1, 8):
            count = len([p for p in in_progress if p.current_phase == phase])
            if count > 0:
                print(f"  Phase {phase}: {count}")

    # Show ready prospects
    if ready:
        print(f"\nReady for DM ({len(ready)}):")
        for p in ready[:10]:
            print(f"  @{p.instagram_handle} - {p.page_name}")
        if len(ready) > 10:
            print(f"  ... and {len(ready) - 10} more")

    print()


def main():
    parser = argparse.ArgumentParser(
        description='Instagram Warm-Up Orchestrator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Initialize warm-up for new prospects
  python warmup_orchestrator.py --init --csv output/prospects_final.csv

  # Run daily warm-up actions (dry run first)
  python warmup_orchestrator.py --daily --dry-run --limit 5

  # Check status
  python warmup_orchestrator.py --status
        """
    )

    parser.add_argument('--init', action='store_true',
                       help='Initialize warm-up by importing prospects from CSV')
    parser.add_argument('--daily', action='store_true',
                       help='Run daily warm-up actions (automated)')
    parser.add_argument('--manual', action='store_true',
                       help='Generate manual checklist for today')
    parser.add_argument('--mark-done', action='store_true',
                       help='Mark today\'s manual tasks as complete')
    parser.add_argument('--status', action='store_true',
                       help='Show warm-up status')
    parser.add_argument('--csv', type=str,
                       help='Path to prospects CSV (for --init)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview actions without executing')
    parser.add_argument('--limit', type=int,
                       help='Limit number of actions/prospects')
    parser.add_argument('--save-config', action='store_true',
                       help='Save default config to file')

    args = parser.parse_args()

    # Load config
    config = load_config()

    if args.save_config:
        save_config(DEFAULT_CONFIG)
        print(f"Config saved to {CONFIG_FILE}")
        return

    # Initialize tracker
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    tracker = WarmupTracker(STATE_FILE)

    # Handle commands
    if args.init:
        if not args.csv:
            print("ERROR: --csv is required with --init")
            sys.exit(1)

        csv_path = Path(args.csv)
        if not csv_path.is_absolute():
            csv_path = BASE_DIR / args.csv

        count = init_warmup(csv_path, tracker)
        print(f"\nImported {count} prospects for warm-up")
        print(f"State saved to: {STATE_FILE}")

    elif args.daily:
        if args.dry_run:
            print("MODE: DRY RUN (no actions will be executed)\n")

        limits = config.get('limits', DEFAULT_CONFIG['limits'])
        actions = WarmupActions(
            dry_run=args.dry_run,
            min_delay=limits['min_delay_seconds'],
            max_delay=limits['max_delay_seconds']
        )
        comment_gen = CommentGenerator()

        print(f"Running daily warm-up...")
        stats = run_daily_warmup(
            tracker=tracker,
            actions=actions,
            comment_gen=comment_gen,
            config=config,
            limit=args.limit,
            dry_run=args.dry_run
        )

        print("\n" + "=" * 60)
        print("DAILY WARM-UP SUMMARY")
        print("=" * 60)
        print(f"Follows:       {stats['follows']}")
        print(f"Likes:         {stats['likes']}")
        print(f"Comments:      {stats['comments']}")
        print(f"Ready for DM:  {stats['ready_for_dm']}")
        print(f"Errors:        {stats['errors']}")
        print("=" * 60)

    elif args.manual:
        # Generate manual checklist
        output_file = BASE_DIR / 'output' / f"warmup_checklist_{datetime.now().strftime('%Y-%m-%d')}.md"
        checklist = generate_manual_checklist(
            tracker=tracker,
            config=config,
            limit=args.limit,
            output_file=output_file
        )
        print(checklist)
        print(f"\n📋 Checklist saved to: {output_file}")

    elif args.mark_done:
        # Mark today's tasks as complete
        advanced = mark_daily_done(tracker, config)
        print(f"\n✅ Marked {advanced} prospects as done for today")
        print(f"Run --status to see updated progress")
        print(f"Run --manual tomorrow for the next checklist")

    elif args.status:
        print_status(tracker)

    else:
        parser.print_help()


if __name__ == '__main__':
    main()
