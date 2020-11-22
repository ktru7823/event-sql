package usyd.it.olympics.gui;

import java.awt.BorderLayout;

import javax.swing.JTextPane;

import usyd.it.olympics.OlympicsDBClient;

public class ReportScreen extends GuiScreen {

    private JTextPane reportPane;

    public ReportScreen(OlympicsDBClient r) {
        super(r);
        panel_.setLayout(new BorderLayout(0, 0));
        reportPane = new JTextPane();
        panel_.add(reportPane);

    }

    public void show(String string) {
        reportPane.setText(string);
    }
}
