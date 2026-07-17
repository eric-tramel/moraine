use dialoguer::console::Style;
use std::fmt;
use std::io::{IsTerminal, Write};

use crate::render::{CliOutput, OutputMode};

#[derive(Debug, Clone, Copy)]
pub(crate) struct ProgressStyle {
    enabled: bool,
    rich: bool,
    unicode: bool,
}

impl ProgressStyle {
    pub(crate) fn from_output(output: &CliOutput) -> Self {
        Self::from_capabilities(output, std::io::stderr().is_terminal())
    }

    pub(crate) fn from_capabilities(output: &CliOutput, stderr_is_terminal: bool) -> Self {
        Self {
            enabled: !output.is_json() && stderr_is_terminal,
            rich: output.mode == OutputMode::Rich,
            unicode: output.unicode,
        }
    }

    #[cfg(test)]
    pub(crate) fn disabled() -> Self {
        Self {
            enabled: false,
            rich: false,
            unicode: true,
        }
    }

    pub(crate) fn enabled(self) -> bool {
        self.enabled
    }

    pub(crate) fn rich(self) -> bool {
        self.rich
    }

    pub(crate) fn mark<'a>(
        self,
        unicode: &'a str,
        ascii: &'a str,
        style: Style,
    ) -> impl fmt::Display + 'a {
        if self.rich {
            style
                .for_stderr()
                .apply_to(if self.unicode { unicode } else { ascii })
        } else {
            Style::new()
                .for_stderr()
                .apply_to(if self.unicode { unicode } else { ascii })
        }
    }

    pub(crate) fn label<'a>(self, value: &'a str) -> impl fmt::Display + 'a {
        if self.rich {
            Style::new().white().for_stderr().apply_to(value)
        } else {
            Style::new().for_stderr().apply_to(value)
        }
    }

    pub(crate) fn bold_label<'a>(self, value: &'a str) -> impl fmt::Display + 'a {
        if self.rich {
            Style::new().white().bold().for_stderr().apply_to(value)
        } else {
            Style::new().for_stderr().apply_to(value)
        }
    }

    pub(crate) fn dim<'a>(self, value: &'a str) -> impl fmt::Display + 'a {
        if self.rich {
            Style::new().bright().black().for_stderr().apply_to(value)
        } else {
            Style::new().for_stderr().apply_to(value)
        }
    }

    pub(crate) fn branch<'a>(
        self,
        unicode: &'a str,
        ascii: &'a str,
        style: Style,
    ) -> impl fmt::Display + 'a {
        self.mark(unicode, ascii, style)
    }
}

pub(crate) struct ProgressTree<W> {
    style: ProgressStyle,
    writer: W,
    started: bool,
    active: bool,
    finished: bool,
}

impl ProgressTree<std::io::Stderr> {
    pub(crate) fn from_output(output: &CliOutput) -> Self {
        Self::new(ProgressStyle::from_output(output), std::io::stderr())
    }
}

impl<W: Write> ProgressTree<W> {
    pub(crate) fn new(style: ProgressStyle, writer: W) -> Self {
        Self {
            style,
            writer,
            started: false,
            active: false,
            finished: false,
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.style.enabled()
    }

    pub(crate) fn start(&mut self, title: &str) {
        if !self.enabled() || self.started {
            return;
        }
        self.started = true;
        if self.style.rich() {
            let _ = writeln!(self.writer);
            let _ = writeln!(
                self.writer,
                "{} {}",
                self.style.branch("╭─", ".-", Style::new().cyan()),
                Style::new().cyan().bold().for_stderr().apply_to(title)
            );
        } else {
            let _ = writeln!(self.writer, "{title}");
        }
    }

    pub(crate) fn phase(&mut self, label: &str, detail: Option<&str>) {
        if !self.enabled() {
            return;
        }
        self.clear_active();
        match detail {
            Some(detail) => {
                let _ = writeln!(
                    self.writer,
                    "{} {} {}",
                    self.style.branch("├─", "+-", Style::new().bright().black()),
                    self.style.bold_label(label),
                    self.style.dim(detail)
                );
            }
            None => {
                let _ = writeln!(
                    self.writer,
                    "{} {}",
                    self.style.branch("├─", "+-", Style::new().bright().black()),
                    self.style.bold_label(label)
                );
            }
        }
    }

    pub(crate) fn step(
        &mut self,
        unicode_mark: &str,
        ascii_mark: &str,
        label: &str,
        detail: Option<&str>,
        style: Style,
    ) {
        if !self.enabled() {
            return;
        }
        self.clear_active();
        match detail {
            Some(detail) => {
                let _ = writeln!(
                    self.writer,
                    "   {} {} {}",
                    self.style.mark(unicode_mark, ascii_mark, style),
                    self.style.label(label),
                    self.style.dim(detail)
                );
            }
            None => {
                let _ = writeln!(
                    self.writer,
                    "   {} {}",
                    self.style.mark(unicode_mark, ascii_mark, style),
                    self.style.label(label)
                );
            }
        }
    }

    pub(crate) fn active(&mut self, label: &str) {
        if !self.enabled() {
            return;
        }
        if self.style.rich() {
            let _ = write!(
                self.writer,
                "\r\u{1b}[2K   {} {}",
                self.style.mark("→", ">", Style::new().cyan()),
                self.style.label(label)
            );
            let _ = self.writer.flush();
            self.active = true;
        } else if !self.active {
            let _ = writeln!(
                self.writer,
                "   {} {}",
                self.style.mark("→", ">", Style::new().cyan()),
                self.style.label(label)
            );
            self.active = true;
        }
    }

    pub(crate) fn clear_active(&mut self) {
        if !self.active {
            return;
        }
        if self.style.rich() {
            let _ = write!(self.writer, "\r\u{1b}[2K");
            let _ = self.writer.flush();
        }
        self.active = false;
    }

    pub(crate) fn finish(&mut self, success: bool, summary: &str) {
        if !self.enabled() || !self.started || self.finished {
            return;
        }
        self.clear_active();
        let style = if success {
            Style::new().green()
        } else {
            Style::new().red()
        };
        let _ = writeln!(
            self.writer,
            "{} {}",
            self.style.branch("╰─", "`-", style),
            self.style.dim(summary)
        );
        self.finished = true;
    }

    #[cfg(test)]
    pub(crate) fn into_inner(self) -> W {
        self.writer
    }
}

#[cfg(test)]
impl ProgressTree<Vec<u8>> {
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.writer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::OutputMode;

    fn output(mode: OutputMode, unicode: bool) -> CliOutput {
        CliOutput {
            mode,
            verbose: false,
            unicode,
            width: 100,
        }
    }

    #[test]
    fn progress_gate_requires_terminal_non_json_stderr() {
        assert!(ProgressStyle::from_capabilities(&output(OutputMode::Rich, true), true).enabled());
        assert!(
            !ProgressStyle::from_capabilities(&output(OutputMode::Rich, true), false).enabled()
        );
        assert!(!ProgressStyle::from_capabilities(&output(OutputMode::Json, true), true).enabled());
    }

    #[test]
    fn progress_symbols_follow_unicode_capability() {
        let unicode = ProgressStyle::from_capabilities(&output(OutputMode::Plain, true), true);
        let ascii = ProgressStyle::from_capabilities(&output(OutputMode::Plain, false), true);

        assert_eq!(format!("{}", unicode.mark("✓", "[ok]", Style::new())), "✓");
        assert_eq!(format!("{}", ascii.mark("✓", "[ok]", Style::new())), "[ok]");
    }
}
