/**
 * AL-28: the refresh indicator for a server-paged table (Linear/Vercel's top
 * bar). A page, filter or retry change keeps the rows already on screen and
 * draws this 2px indeterminate bar on the table's top edge, instead of the
 * AntD Spin overlay that blanked the whole table.
 *
 * Mount it directly above the `<Table>`. It renders nothing while idle and,
 * while busy, overlaps the header's first 2px with a negative margin, so it
 * never moves the rows. The progressbar has no `aria-valuenow`, which makes it
 * indeterminate: assistive tech announces the busy state from the label. Under
 * `prefers-reduced-motion` the sweep stops and a static muted band remains
 * (admin.css), so the loading hint survives without animation.
 */
export function TableProgressBar({ loading, label }: { loading: boolean; label: string }) {
  if (!loading) return null;
  return (
    <div
      className="xflow-admin-table-progress"
      role="progressbar"
      aria-busy="true"
      aria-label={label}
    />
  );
}
