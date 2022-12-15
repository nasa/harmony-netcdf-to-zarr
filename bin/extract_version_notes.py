""" A Python module that will read the contents of CHANGELOG.md and extract
    the release notes for the most recent version.

    This module assumes:

    * The top entry in CHANGELOG.md is that of the release to be published.
    * Each entry is separated by a new line and starts with a line: "## vX.Y.Z"

"""
if __name__ == '__main__':
    with open('CHANGELOG.md', 'r', encoding='utf-8') as file_handler:
        changelog_lines = file_handler.readlines()

    split_index = next((line_index
                       for line_index, line
                       in enumerate(changelog_lines[:-2])
                       if line == '\n'
                       and changelog_lines[line_index + 1].startswith('## v')),
                       None)

    with open('version_notes.md', 'w', encoding='utf-8') as file_handler:
        file_handler.writelines(changelog_lines[:split_index])
