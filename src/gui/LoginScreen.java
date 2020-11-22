package usyd.it.olympics.gui;

import java.awt.Component;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Arrays;
import javax.swing.*;
import usyd.it.olympics.OlympicsDBClient;

public class LoginScreen extends GuiScreen {

    private final JTextField txtMemberName;
    private final JPasswordField pwdMemberUser;

    public LoginScreen(OlympicsDBClient r) {
        super(r);
        panel_.setLayout(new GridLayout(3, 2, 0, 0));

        JLabel lblMemberName = new JLabel("Member ID");
        panel_.add(lblMemberName);

        txtMemberName = new JTextField();
        txtMemberName.setText("");
        panel_.add(txtMemberName);
        txtMemberName.setColumns(10);

        JLabel lblPassword = new JLabel("Password");
        panel_.add(lblPassword);

        pwdMemberUser = new JPasswordField();
        pwdMemberUser.setText("");
        panel_.add(pwdMemberUser);
        pwdMemberUser.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent arg0) {
                submit();
            }
        });

        Component horizontalGlue = Box.createHorizontalGlue();
        panel_.add(horizontalGlue);

        JButton btnLogIn = new JButton("Log In");
        btnLogIn.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent arg0) {
                submit();
            }
        });
        panel_.add(btnLogIn);

    }
    
    private void submit() {
        char[] input = pwdMemberUser.getPassword();
        client_.login(txtMemberName.getText(), input);
        Arrays.fill(input, '0');
        pwdMemberUser.setText("");  
    }
}
