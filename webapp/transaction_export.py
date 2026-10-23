"""Bounded CSV rendering for a read-only transaction snapshot."""
import csv
import io
import re


class ExportTooLarge(ValueError):
    pass


def csv_text(value):
    text = '' if value is None else str(value)
    # Only text is escaped. Signed amounts remain exact numeric decimal cells.
    if re.match(r'^\s*[=+@-]|^[\t\r\n]', text):
        text = "'" + text
    return text


def transactions_csv(rows, *, max_rows=50_000, max_bytes=10 * 1024 * 1024):
    """Consume a cursor incrementally and fail instead of returning a partial file."""
    output = io.BytesIO(b'\xef\xbb\xbf')
    output.seek(0, io.SEEK_END)
    line = io.StringIO(newline='')
    writer = csv.writer(line, quoting=csv.QUOTE_ALL, lineterminator='\r\n')

    def append(values):
        line.seek(0)
        line.truncate()
        writer.writerow(values)
        encoded = line.getvalue().encode('utf-8')
        if output.tell() + len(encoded) > max_bytes:
            raise ExportTooLarge('Export is too large. Narrow the filters and try again.')
        output.write(encoded)

    append(['ID', 'Date', 'Description', 'Amount', 'Source', 'Card last 4',
            'Primary category', 'Tags', 'Type', 'Status', 'Import file', 'Needs review'])
    count = 0
    for row in rows:
        count += 1
        if count > max_rows:
            raise ExportTooLarge(f'Export is limited to {max_rows:,} transactions. Narrow the filters and try again.')
        append([
            str(row['id']), csv_text(row['date']), csv_text(row['description']),
            format(row['amount'], '.2f'), csv_text(row['source']), csv_text(row['card_last4']),
            csv_text(row['primary_tag']), csv_text('; '.join(row['tags'] or [])),
            csv_text(row['transaction_type']), csv_text(row['status']),
            csv_text(row['import_file']), 'Yes' if row['needs_review'] else 'No',
        ])
    return output.getvalue(), count
