// Program side of the tools/pt-trace control protocol, in plain C.
//
// This is the C twin of modules/pt/include/pt/pt_control.h: pt-trace starts
// perf with the trace disabled and hands us two fifos through the
// environment. We write "enable\n" / "disable\n" to PERF_CTL_FIFO and wait
// for perf's ack on PERF_ACK_FIFO, so the recorded region is exactly the one
// we bracket. With the variables unset both calls are no-ops and the program
// runs untraced.
#pragma once

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int pt_ctl_fd = -1;
static int pt_ack_fd = -1;

static void pt_ctl_open(void) {
  const char *ctl = getenv("PERF_CTL_FIFO");
  const char *ack = getenv("PERF_ACK_FIFO");
  if (!ctl || !ack) {
    return;
  }
  pt_ctl_fd = open(ctl, O_WRONLY);
  pt_ack_fd = open(ack, O_RDONLY);
}

static void pt_ctl_send(const char *cmd) {
  if (pt_ctl_fd < 0 || pt_ack_fd < 0) {
    return;
  }
  if (write(pt_ctl_fd, cmd, strlen(cmd)) < 0) {
    return;
  }
  // perf acks once the command has taken effect, so on return the trace is
  // guaranteed to be on (or off).
  char ack[16];
  (void)!read(pt_ack_fd, ack, sizeof(ack));
}

static void pt_enable(void) {
  if (pt_ctl_fd < 0) {
    pt_ctl_open();
  }
  pt_ctl_send("enable\n");
}

static void pt_disable(void) { pt_ctl_send("disable\n"); }
