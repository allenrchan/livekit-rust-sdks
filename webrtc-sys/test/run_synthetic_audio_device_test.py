#!/usr/bin/env python3
"""Build a standalone deterministic native-pump regression on macOS.

Uses the caller's exact SDK-compatible WebRTC artifact, including its recorded
defines. This is not a substitute for platform Cargo or real-room qualification.
"""
import argparse
from pathlib import Path
import re
import resource
import subprocess
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--webrtc-dir', type=Path, required=True)
parser.add_argument('--build-dir', type=Path, required=True)
parser.add_argument('--case', default='all', choices=(
    'all', 'no_transport', 'registration_lifecycle', 'restart_cleanup'))
args = parser.parse_args()
if sys.platform != 'darwin':
    parser.error('this standalone runner supports macOS; use platform qualification elsewhere')
root = Path(__file__).resolve().parents[1]
artifact = args.webrtc_dir.resolve(strict=True)
output = args.build_dir.resolve()
if output == root or output.is_relative_to(root):
    parser.error('build output must be outside SDK source')
output.mkdir(parents=True, exist_ok=True)
defines = set()
for name in ('webrtc.ninja', 'desktop_capture.ninja'):
    line = (artifact / name).read_text().splitlines()[0]
    for key, value in re.findall(r'-D(\w+)(?:=([^\s]+))?', line):
        if key not in ('CR_CLANG_REVISION', 'CR_XCODE_VERSION'):
            defines.add('-D' + key + ('=' + value if value else ''))
include = artifact / 'include'
command = ['clang++', '-std=c++20', '-stdlib=libc++', '-Wl,-ObjC',
           '-Wno-nullability-completeness', *sorted(defines)]
for path in (root / 'include', include, include / 'third_party/abseil-cpp',
             include / 'third_party/libyuv/include', include / 'sdk/objc',
             include / 'sdk/objc/base'):
    command += ['-I', str(path)]
binary = output / 'synthetic_audio_device_test'
command += [str(root / 'test/synthetic_audio_device_test.cc'),
            str(root / 'src/synthetic_audio_device.cpp'),
            str(artifact / 'lib/libwebrtc.a'), '-o', str(binary)]
for framework in ('Foundation', 'AVFoundation', 'Security', 'CoreAudio',
                  'AudioToolbox', 'AppKit', 'CoreMedia', 'CoreGraphics',
                  'VideoToolbox', 'CoreVideo', 'OpenGL', 'Metal', 'MetalKit',
                  'QuartzCore', 'IOKit', 'IOSurface', 'ScreenCaptureKit'):
    command += ['-framework', framework]
subprocess.run(command, check=True)
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
result = subprocess.run([str(binary), args.case], check=False)
raise SystemExit(128 - result.returncode if result.returncode < 0 else result.returncode)
