package usyd.it.olympics.gui;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import javax.swing.table.DefaultTableCellRenderer;

/// Display date and time in JTable
class DateTimeRenderer extends DefaultTableCellRenderer {
    DateFormat formatter;
    public DateTimeRenderer() { super(); }

    @Override
    public void setValue(Object value) {
        if (formatter==null) {
            formatter = new SimpleDateFormat("dd-MMM-yyyy h:mm a");
        }
        setText((value == null) ? "" : formatter.format(value));
    }
}