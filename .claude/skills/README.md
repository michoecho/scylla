# Skills

Project skills for Claude Code: `modules`, `pt-trace`,
`snapshot-tests`. Each is a directory with a `SKILL.md` holding the procedure
and the frontmatter (`name`, `description`) Claude matches against a request.

They live here because `.claude/skills/` is the only place Claude Code
discovers project skills. `skills` at the repository root is a symlink to this
directory, so they stay reachable — and browsable — from the top level without
a second copy.

## Adding a skill

Nothing to register: write the directory here and Claude Code finds it.

```sh
mkdir -p .claude/skills/my-skill               # write its SKILL.md
git add .claude/skills/my-skill
```

It shows up under the top-level `skills/` link automatically, since that link
points at this whole directory.

Restart Claude Code afterwards — skills are enumerated at startup, so a new one
is not visible mid-session.

## Why not the other way around

Putting the real directories at the top level and symlinking
`.claude/skills -> ../skills` does **not** work, and is why these skills went
undiscovered for a while. Claude Code lets an individual `<skill-name>` entry be
a symlink, but not the `.claude/skills` directory itself. Keeping the content
here and pointing the cosmetic link inward avoids that: if the top-level link is
ever broken or dropped, discovery still works.

There is no settings.json alternative — `permissions.additionalDirectories`
grants file access without loading skills, and the `--add-dir` exception looks
for `.claude/skills/` *inside* the added directory.
