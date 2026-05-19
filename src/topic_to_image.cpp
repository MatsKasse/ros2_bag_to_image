/*
 *  Copyright (c) 2023, MAP IV.
 *  All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 *  FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 *  DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 *  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 *  CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 *  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *  v1.0 amc-nu 2023-08
 */

#include "topic_to_image/topic_to_image.hpp"

#include <fcntl.h>
#include <sys/select.h>
#include <termios.h>
#include <unistd.h>

TopicToImage::TopicToImage(const rclcpp::NodeOptions & options) :
  Node("topic_to_image", options)
{
  // IO
  input_topic_ = this->declare_parameter<std::string>("input/topic", "/image_raw");
  output_path_ = this->declare_parameter<std::string>("output/path", "/tmp/");
  file_prefix_ = this->declare_parameter<std::string>("output/prefix", "");

  bool compressed = this->declare_parameter<bool>("compressed", false);
  std::string image_transport = "raw";
  if (compressed) {
    image_transport = "compressed";
  }
  image_sub_ = image_transport::create_subscription(this,
                                                    input_topic_,
                                                    std::bind(&TopicToImage::ImageCallback, this, std::placeholders::_1),
                                                    image_transport,
                                                    rmw_qos_profile_sensor_data
  );
  RCLCPP_INFO_STREAM(get_logger(), "Using image_transport: '" << image_transport << "' on " << image_sub_.getTopic());
  if(output_path_.empty()) {
    RCLCPP_ERROR_STREAM(get_logger(), "Invalid Path provided:" << output_path_ << ". Terminating...");
    rclcpp::shutdown(nullptr, "Invalid Output Path");
  }
  if (!std::filesystem::exists(output_path_)) {
    RCLCPP_INFO_STREAM(get_logger(), "The Provided path [" << output_path_ << "]doesn't exist. Trying to create.");
    if (std::filesystem::create_directories(output_path_)){
      RCLCPP_INFO_STREAM(get_logger(), "The Provided Path was created successfully.");
    }
    else {
      RCLCPP_ERROR_STREAM(get_logger(), "Could not create the output directory: " << output_path_ << ". Terminating");
      rclcpp::shutdown(nullptr, "Missing Permissions on the output path");
    }
  }
  keyboard_thread_running_ = true;
  keyboard_thread_ = std::thread(&TopicToImage::KeyboardLoop, this);

  RCLCPP_INFO_STREAM(get_logger(), "Saving Images PNGs to:" << output_path_ << ",  with prefix:" << file_prefix_);
  RCLCPP_INFO_STREAM(get_logger(), "Press 's' in this terminal to save the latest received image.");
}

TopicToImage::~TopicToImage()
{
  keyboard_thread_running_ = false;
  if (keyboard_thread_.joinable()) {
    keyboard_thread_.join();
  }
}

void TopicToImage::ImageCallback(const sensor_msgs::msg::Image::ConstSharedPtr & image_msg)
{
  std::lock_guard<std::mutex> lock(latest_image_mutex_);
  latest_image_ = image_msg;
}

void TopicToImage::KeyboardLoop()
{
  keyboard_fd_ = open("/dev/tty", O_RDONLY | O_NONBLOCK);
  if (keyboard_fd_ < 0) {
    keyboard_fd_ = STDIN_FILENO;
  }

  if (!isatty(keyboard_fd_)) {
    RCLCPP_WARN_STREAM(get_logger(), "No interactive terminal available. Press-to-save keyboard input is disabled.");
    if (keyboard_fd_ != STDIN_FILENO) {
      close(keyboard_fd_);
    }
    keyboard_fd_ = -1;
    return;
  }

  termios original_terminal_settings;
  if (tcgetattr(keyboard_fd_, &original_terminal_settings) != 0) {
    RCLCPP_WARN_STREAM(get_logger(), "Could not read terminal settings. Press-to-save keyboard input is disabled.");
    if (keyboard_fd_ != STDIN_FILENO) {
      close(keyboard_fd_);
    }
    keyboard_fd_ = -1;
    return;
  }

  termios raw_terminal_settings = original_terminal_settings;
  raw_terminal_settings.c_lflag &= static_cast<tcflag_t>(~(ICANON | ECHO));
  raw_terminal_settings.c_cc[VMIN] = 0;
  raw_terminal_settings.c_cc[VTIME] = 0;

  if (tcsetattr(keyboard_fd_, TCSANOW, &raw_terminal_settings) != 0) {
    RCLCPP_WARN_STREAM(get_logger(), "Could not configure terminal input. Press-to-save keyboard input is disabled.");
    if (keyboard_fd_ != STDIN_FILENO) {
      close(keyboard_fd_);
    }
    keyboard_fd_ = -1;
    return;
  }

  while (keyboard_thread_running_ && rclcpp::ok()) {
    fd_set read_fds;
    FD_ZERO(&read_fds);
    FD_SET(keyboard_fd_, &read_fds);

    timeval timeout;
    timeout.tv_sec = 0;
    timeout.tv_usec = 100000;

    const int ready = select(keyboard_fd_ + 1, &read_fds, nullptr, nullptr, &timeout);
    if (ready <= 0 || !FD_ISSET(keyboard_fd_, &read_fds)) {
      continue;
    }

    char key;
    if (read(keyboard_fd_, &key, 1) == 1 && (key == 's' || key == 'S')) {
      SaveLatestImage();
    }
  }

  tcsetattr(keyboard_fd_, TCSANOW, &original_terminal_settings);
  if (keyboard_fd_ != STDIN_FILENO) {
    close(keyboard_fd_);
  }
  keyboard_fd_ = -1;
}

void TopicToImage::SaveLatestImage()
{
  sensor_msgs::msg::Image::ConstSharedPtr image_msg;
  {
    std::lock_guard<std::mutex> lock(latest_image_mutex_);
    image_msg = latest_image_;
  }

  if (!image_msg) {
    RCLCPP_WARN_STREAM(get_logger(), "No image received yet. Nothing saved.");
    return;
  }

  SaveImage(image_msg);
}

void TopicToImage::SaveImage(const sensor_msgs::msg::Image::ConstSharedPtr & image_msg)
{
  std::string fname;
  fname = output_path_ + "/" + file_prefix_ + "_" +
      boost::lexical_cast<std::string>(image_msg->header.stamp.sec) + "."+
          boost::lexical_cast<std::string>(image_msg->header.stamp.nanosec) + ".png";

  auto output_encoding = sensor_msgs::image_encodings::BGR8;
  if (sensor_msgs::image_encodings::numChannels(image_msg->encoding) == 1) {
    if (sensor_msgs::image_encodings::bitDepth(image_msg->encoding) == 8) {
      output_encoding = sensor_msgs::image_encodings::TYPE_8UC1;
    } else {
      output_encoding = sensor_msgs::image_encodings::TYPE_16UC1;
    }
  }
  cv_bridge::CvImagePtr in_image_ptr;
  try {
    in_image_ptr = cv_bridge::toCvCopy(image_msg, output_encoding);
  } catch (cv_bridge::Exception & e) {
    RCLCPP_ERROR(this->get_logger(), "cv_bridge exception: %s", e.what());
    return;
  }
  cv::imwrite(fname, in_image_ptr->image);

  RCLCPP_INFO_STREAM(get_logger(), "Image saved to: " << fname);
}

#include <rclcpp_components/register_node_macro.hpp>
RCLCPP_COMPONENTS_REGISTER_NODE(TopicToImage)
