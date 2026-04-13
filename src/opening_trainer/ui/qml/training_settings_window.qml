import QtQuick
import QtQuick.Window

Window {
    id: root
    width: 920
    height: 640
    visible: true
    title: "Opening Trainer - Training Settings"
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
