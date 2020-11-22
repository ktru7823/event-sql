package usyd.it.olympics.gui;

import javax.swing.JPanel;
import usyd.it.olympics.OlympicsDBClient;

/**
 *
 * @author Bryn
 */
class GuiScreen {

    protected final JPanel panel_;
    protected final OlympicsDBClient client_;

    /**
     * @return the panel_
     */
    public JPanel getPanel() {
        return panel_;
    }
    
    protected GuiScreen(OlympicsDBClient r) {
        panel_ = new JPanel();
        client_ = r;
    }
}
