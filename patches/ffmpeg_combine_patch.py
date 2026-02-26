"""
Monkey-patch for podcast_creator.core.combine_audio_files
Uses ffmpeg concat demuxer instead of moviepy to avoid opening all files simultaneously.
This fixes [Errno 11] Resource temporarily unavailable errors in constrained containers.
"""
import subprocess
import uuid
from pathlib import Path
from typing import Union

from loguru import logger

import podcast_creator.core as podcast_core


async def combine_audio_files_ffmpeg(
    audio_dir: Union[Path, str],
    final_filename: str,
    final_output_dir: Union[Path, str],
):
    """
    Combines multiple audio files into a single MP3 file using ffmpeg concat demuxer.
    Drop-in replacement for podcast_creator.core.combine_audio_files that avoids
    opening all files simultaneously (which causes resource exhaustion in containers).
    """
    if isinstance(audio_dir, str):
        audio_dir = Path(audio_dir)
    if isinstance(final_output_dir, str):
        final_output_dir = Path(final_output_dir)

    list_of_audio_paths = sorted(audio_dir.glob("*.mp3"))

    if not list_of_audio_paths:
        logger.warning("combine_audio_files_ffmpeg: No audio files found.")
        return {"combined_audio_path": "ERROR: No audio segment data"}

    clip_count = len(list_of_audio_paths)
    logger.info(
        "combine_audio_files_ffmpeg: Found %d clips to combine", clip_count
    )

    # Create output directory
    final_output_dir.mkdir(parents=True, exist_ok=True)

    # Determine output filename
    if final_filename and isinstance(final_filename, str):
        output_filename = Path(final_filename).name
        if not output_filename.endswith(".mp3"):
            output_filename += ".mp3"
    else:
        output_filename = "combined_" + uuid.uuid4().hex + ".mp3"

    output_path = final_output_dir / output_filename

    # Create ffmpeg concat list file
    concat_list_path = audio_dir / "_concat_list.txt"
    try:
        with open(concat_list_path, "w") as f:
            for clip_path in list_of_audio_paths:
                resolved = str(clip_path.resolve())
                line = "file " + repr(resolved) + "\n"
                f.write(line)

        logger.info(
            "combine_audio_files_ffmpeg: Concat list written to %s", concat_list_path
        )

        # Run ffmpeg concat demuxer - processes files sequentially, no simultaneous opens
        cmd = [
            "ffmpeg",
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(concat_list_path),
            "-c",
            "copy",
            str(output_path),
        ]

        logger.info("combine_audio_files_ffmpeg: Running ffmpeg concat...")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
        )

        if result.returncode != 0:
            logger.error("ffmpeg concat failed: %s", result.stderr)
            error_msg = result.stderr[:500]
            return {
                "combined_audio_path": "ERROR: ffmpeg concat failed - " + error_msg
            }

        # Get duration using ffprobe
        duration = 0.0
        try:
            probe_cmd = [
                "ffprobe",
                "-v",
                "quiet",
                "-show_entries",
                "format=duration",
                "-of",
                "csv=p=0",
                str(output_path),
            ]
            probe_result = subprocess.run(
                probe_cmd, capture_output=True, text=True, timeout=30
            )
            if probe_result.returncode == 0 and probe_result.stdout.strip():
                duration = float(probe_result.stdout.strip())
        except Exception as e:
            logger.warning("Could not get duration: %s", e)

        logger.info(
            "combine_audio_files_ffmpeg: Successfully combined audio to %s (duration: %.1fs)",
            output_path,
            duration,
        )

        return {
            "combined_audio_path": str(output_path.resolve()),
            "original_segments_count": len(list_of_audio_paths),
            "total_duration_seconds": duration,
        }

    except Exception as e:
        logger.error("combine_audio_files_ffmpeg: Error: %s", e)
        return {"combined_audio_path": "ERROR: Failed to combine audio - " + str(e)}

    finally:
        # Clean up concat list
        if concat_list_path.exists():
            concat_list_path.unlink()


def apply_patch():
    """Apply the monkey-patch to replace moviepy-based combine with ffmpeg-based."""
    podcast_core.combine_audio_files = combine_audio_files_ffmpeg
    logger.info(
        "Patched podcast_creator.core.combine_audio_files -> ffmpeg concat demuxer "
        "(avoids simultaneous file opens)"
    )


# Auto-apply when imported
apply_patch()
