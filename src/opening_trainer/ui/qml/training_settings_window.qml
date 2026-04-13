import QtQuick
import QtQuick.Controls

ApplicationWindow {
    id: root
    title: "Opening Trainer - Training Settings"
    width: 920
    height: 640
    visible: true
    color: "#1f1f1f"

    Rectangle {
        anchors.fill: parent
        color: "#1f1f1f"

        Text {
            anchors.centerIn: parent
            text: "Qt Quick baseline window"
            color: "#aaaaaa"
            font.pixelSize: 14
        }
    }
}
